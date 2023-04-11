CREATE TEMPORARY TABLE IF NOT EXISTS team_data AS (
  SELECT t.date,
    t.team,
    t.opponent,
    SUM(t.points_scored) AS team_points,
    SUM(t.assists) AS team_ast,
    SUM(t.field_goals + t.three_point_field_goals) AS team_fg,
    SUM(t.field_goal_attempts + t.three_point_field_goal_attempts) AS team_fga,
    SUM(t.free_throws) AS team_ft,
    SUM(t.free_throw_attempts) AS team_fta,
    SUM(t.turnovers) AS team_tov,
    SUM(t.offensive_rebounds) AS team_orb,
    SUM(t.minutes_played) AS team_minutes,
    SUM(t.three_point_field_goals) AS team_3fg,
    o.opp_orb,
    o.opp_trb
  FROM nightly_player_totals t
    JOIN (
      SELECT date,
        team,
        SUM(offensive_rebounds) AS opp_orb,
        SUM(offensive_rebounds + defensive_rebounds) AS opp_trb
      FROM nightly_player_totals
      GROUP BY date, team
    ) o ON t.date = o.date
    AND t.opponent = o.team
  GROUP BY t.date,
    t.team,
    t.opponent
);

CREATE TEMPORARY TABLE IF NOT EXISTS player_data AS (
  SELECT p.id,
    p.date,
    p.team,
    p.player_name,
    p.points_scored,
    p.minutes_played,
    p.assists,
    p.free_throws,
    p.free_throw_attempts,
    p.three_point_field_goals,
    p.offensive_rebounds,
    p.turnovers,
    p.steals,
    p.blocks,
    p.personal_fouls,
    SUM(p.offensive_rebounds + p.defensive_rebounds) AS player_trb,
    (p.points_scored + 0.4 * (p.field_goals + p.three_point_field_goals) - 0.7 * (p.field_goal_attempts + p.three_point_field_goal_attempts) - 0.4*(p.free_throw_attempts - p.free_throws) + 0.7 * p.offensive_rebounds + 0.3 * p.defensive_rebounds + p.steals + 0.7 * p.assists + 0.7 * p.blocks - 0.4 * p.personal_fouls - p.turnovers) AS game_score,
    SUM(p.field_goals + p.three_point_field_goals) AS player_fg,
    SUM(p.field_goal_attempts + p.three_point_field_goal_attempts) AS player_fga
  FROM nightly_player_totals p
  GROUP BY p.id,
    p.date,
    p.team,
    p.player_name,
    p.points_scored,
    p.minutes_played,
    p.offensive_rebounds,
    p.assists,
    p.free_throws,
    p.free_throw_attempts,
    p.three_point_field_goals,
    p.turnovers,
    p.steals,
    p.blocks,
    p.personal_fouls
);

CREATE TEMPORARY TABLE IF NOT EXISTS col_1 AS (
  SELECT col.id,
    col.date,
    col.team,
    col.player_name,
    col.opponent,
    team_data.team_points,
    player_data.points_scored,
    (((player_data.minutes_played / (team_data.team_minutes / 5)) * (1.14 * ((team_data.team_ast - player_data.assists) / team_data.team_fg))) + ((((team_data.team_ast / team_data.team_minutes) * player_data.minutes_played * 5 - player_data.assists) / ((team_data.team_fg  / team_data.team_minutes) * player_data.minutes_played * 5 - player_data.player_fg)) * (1 - (player_data.minutes_played / (team_data.team_minutes / 5))))) AS qAST,
    (0.5 * (((team_data.team_points - team_data.team_ft) - (player_data.points_scored - player_data.free_throws)) / (2 * (team_data.team_fga - player_data.player_fga))) * player_data.assists) AS AST_Part,
    COALESCE(((1-POWER(1-(NULLIF(player_data.free_throws,0)/player_data.free_throw_attempts),2))*0.4*player_data.free_throw_attempts),0) AS FT_Part,
    (team_data.team_fg + (1 - POWER(1 - (team_data.team_ft / team_data.team_fta),2)) * team_data.team_fta * 0.4) AS Team_Scoring_Poss,
    (team_data.team_orb / (team_data.team_orb + (team_data.opp_trb - team_data.opp_orb))) AS team_orb_pct,
    COALESCE(((POWER(1 - (NULLIF(player_data.free_throws,0) / player_data.free_throw_attempts),2)) * 0.4 * player_data.free_throw_attempts),0) AS FTxPoss, -- missed free throw possessions 
    (2 * ((team_data.team_fg - player_data.player_fg + 0.5 * (team_data.team_3fg - player_data.three_point_field_goals)) / (team_data.team_fg - player_data.player_fg)) * 0.5 * (((team_data.team_points - team_data.team_ft) - (player_data.points_scored - player_data.free_throws)) / (2 * (team_data.team_fga - player_data.player_fga))) * player_data.assists) AS PProd_AST_Part
  FROM nightly_player_totals col
    LEFT JOIN team_data ON col.date = team_data.date
    AND col.team = team_data.team
    LEFT JOIN player_data ON col.id = player_data.id
    ORDER BY col.date, col.team
);

CREATE TEMPORARY TABLE IF NOT EXISTS col_2 AS (
SELECT col.id,
  col.date,
  col.team,
  col.player_name,
  col_1.team_orb_pct,
  player_data.offensive_rebounds AS offensive_rebounds,
  COALESCE((player_data.player_fg * (1 - 0.5 * (NULLIF((player_data.points_scored - player_data.free_throws),0) / (2 * player_data.player_fga)) * col_1.qAST)),0) AS FG_Part,
  (col_1.Team_Scoring_Poss / (team_data.team_fga + team_data.team_fta * 0.4 + team_data.team_tov)) AS team_play_pct,
  ((player_data.player_fga - player_data.player_fg) * (1 - 1.07 * col_1.team_orb_pct)) AS FGxPoss, -- missed field goal possessions
  COALESCE((2 * (player_data.player_fg + 0.5 * player_data.three_point_field_goals) * (1 - 0.5 * (NULLIF((player_data.points_scored - player_data.free_throws),0) / (2 * player_data.player_fga)) * col_1.qAST)),0) AS PProd_FG_Part
FROM nightly_player_totals col
  JOIN col_1 ON col.id = col_1.id
  LEFT JOIN player_data ON col.id = player_data.id
  LEFT JOIN team_data ON col.date = team_data.date AND col.team = team_data.team
);

WITH cte_one AS(
  SELECT m.id,
    col_2.team_play_pct,
    col_2.FG_Part,
    col_2.FGxPoss,
    col_2.PProd_FG_Part, 
    (((1 - col_2.team_orb_pct) * col_2.team_play_pct) / ((1 - col_2.team_orb_pct) * col_2.team_play_pct + col_2.team_orb_pct * (1 - col_2.team_play_pct))) AS Team_ORB_Weight
  FROM nightly_player_totals m
    JOIN col_2 ON m.id = col_2.id
),
cte_two AS (
  SELECT cte.id,
    cte_one.FG_Part,
    cte_one.Team_ORB_Weight,
    cte_one.team_play_pct,
    cte_one.FGxPoss,
    cte_one.PProd_FG_Part,
    player_data.turnovers,
    player_data.offensive_rebounds,
    player_data.free_throws,
    player_data.free_throw_attempts,
    player_data.player_fga,
    player_data.player_fg,
    player_data.minutes_played,
    player_data.game_score,
    player_data.assists,
    player_data.player_trb,
    player_data.steals,
    player_data.blocks,
    player_data.personal_fouls,
    player_data.three_point_field_goals,
    (player_data.offensive_rebounds * cte_one.Team_ORB_Weight * cte_one.team_play_pct) AS ORB_Part,
    ((cte.field_goals + 0.5 * cte.three_point_field_goals) / player_data.player_fga) AS eff_fg_pct
  FROM nightly_player_totals cte
    JOIN player_data ON cte.id = player_data.id
    JOIN cte_one ON cte.id = cte_one.id
),
cte_three AS (
  SELECT cte.id,
    cte.date,
    cte.team,
    col_1.FTxPoss,
    col_1.PProd_AST_Part,
    col_1.Team_Scoring_Poss,
    cte_two.FGxPoss,
    cte_two.turnovers,
    cte_two.PProd_FG_Part,
    cte_two.free_throws,
    cte_two.Team_ORB_Weight,
    cte_two.team_play_pct,
    team_data.team_orb,
    cte_two.eff_fg_pct,
    cte_two.game_score,
    (100 * ((cte_two.player_fga + 0.44 * cte_two.free_throw_attempts + cte_two.turnovers) * (team_data.team_minutes / 5)) / (cte_two.minutes_played * (team_data.team_fga + 0.44 * team_data.team_fta + team_data.team_tov))) AS usage_rate,
    ((cte_two.FG_Part + col_1.AST_Part + col_1.FT_Part) * (1 - (team_data.team_orb / col_1.Team_Scoring_Poss) * cte_two.Team_ORB_Weight * cte_two.team_play_pct) + cte_two.ORB_Part) AS ScPoss,
    (cte_two.offensive_rebounds * cte_two.Team_ORB_Weight * cte_two.team_play_pct * (team_data.team_points / (team_data.team_fg + (1 - POWER((1 - (team_data.team_ft/ team_data.team_fta)),2)) * 0.4 * team_data.team_fta))) AS PProd_ORB_Part
  FROM nightly_player_totals cte
    JOIN col_1 ON cte.id = col_1.id
    JOIN team_data ON cte.date = team_data.date
    AND cte.team = team_data.team
    JOIN cte_two ON cte.id = cte_two.id
),
cte_four AS ( 
  SELECT cte.id,
    cte.player_name,
    cte.team,
    cte_three.PProd_ORB_Part,
    cte_three.usage_rate,
    cte_three.eff_fg_pct,
    cte_three.game_score,
    (cte_three.ScPoss + cte_three.FGxPoss + cte_three.FTxPoss + cte_three.turnovers) AS TotPoss,
    ((cte_three.PProd_FG_Part + cte_three.PProd_AST_Part + cte_three.free_throws) * (1 - (cte_three.team_orb / cte_three.Team_Scoring_Poss) * cte_three.Team_ORB_Weight * cte_three.team_play_pct) + cte_three.PProd_ORB_Part) AS PProd,
    cte_three.ScPoss
  FROM nightly_player_totals cte
    JOIN cte_three ON cte.id = cte_three.id
),
fin AS (
  SELECT m.id,
    m.date,
    m.player_name,
    m.team,
    m.opponent,
    cte_four.usage_rate,
    cte_four.game_score,
    COALESCE( cte_four.eff_fg_pct,0) AS eff_fg_pct,
    COALESCE((100 * (cte_four.PProd / cte_four.TotPoss)),0) AS ORtg -- offensive rating
  FROM nightly_player_totals m
  JOIN cte_four ON m.id = cte_four.id
)
SELECT id,
  date,
  team,
  opponent,
  player_name,
  game_score,
  usage_rate,
  eff_fg_pct,
  ORtg
FROM fin
ORDER BY date, team






