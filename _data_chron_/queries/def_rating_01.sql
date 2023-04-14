CREATE TEMPORARY TABLE IF NOT EXISTS team_data AS (
  SELECT t.date,
    t.team,
    t.opponent,
    SUM(t.defensive_rebounds) AS team_drb, 
    SUM(t.minutes_played) AS team_minutes, 
    SUM(t.personal_fouls) AS team_pf, 
    SUM(t.blocks) AS team_blks, 
    SUM(t.steals) AS team_stls,
    SUM(t.field_goal_attempts + t.three_point_field_goal_attempts) AS team_fga,
    SUM(t.field_goals + three_point_field_goals) AS team_fg,
    SUM(t.free_throw_attempts) AS team_fta,
    SUM(t.offensive_rebounds) AS team_orb,
    SUM(t.turnovers) AS team_tov,
    o.opp_pts,
    o.opp_orb, 
    o.opp_fg, 
    o.opp_fga, 
    o.opp_tov, 
    o.opp_ft, 
    o.opp_fta, 
    o.opp_minutes,
    o.opp_drb
  FROM nightly_player_totals t
    JOIN (
      SELECT date,
        team,
        SUM(points_scored) AS opp_pts,
        SUM(offensive_rebounds) AS opp_orb,
        SUM(field_goals + three_point_field_goals) AS opp_fg,
        SUM(field_goal_attempts + three_point_field_goal_attempts) AS opp_fga,
        SUM(turnovers) AS opp_tov,
        SUM(free_throws) AS opp_ft,
        SUM(free_throw_attempts) AS opp_fta,
        SUM(minutes_played) AS opp_minutes,
        SUM(defensive_rebounds) AS opp_drb
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
    p.minutes_played, 
    p.personal_fouls, 
    p.defensive_rebounds, 
    p.steals, 
    p.blocks
  FROM nightly_player_totals p
  GROUP BY p.id,
    p.date,
    p.team,
    p.player_name,
    p.personal_fouls,
    p.minutes_played,
    p.defensive_rebounds,
    p.steals,
    p.blocks
);
CREATE TEMPORARY TABLE IF NOT EXISTS def_1 AS (
  WITH cte_one AS (
    SELECT d.id,
      d.team,
      d.date,
      team_data.opp_fga,
      team_data.opp_fg,
      team_data.team_blks,
      team_data.opp_tov,
      team_data.team_stls,
      team_data.team_pf,
      team_data.opp_fta,
      team_data.opp_ft,
      team_data.opp_minutes,
      team_data.team_minutes,
      team_data.opp_pts,
      (team_data.opp_orb / (team_data.opp_orb + team_data.team_drb)) AS DOR_pct,
      (team_data.opp_fg / team_data.opp_fga) AS DFG_pct,
      ((team_data.team_fga + 0.4 * team_data.team_fta - 1.07 * (team_data.team_orb  / (team_data.team_orb + team_data.opp_drb)) * team_data.team_fga - team_data.team_fg) + team_data.team_tov) AS team_possessions
    FROM nightly_player_totals d
    JOIN team_data ON d.date = team_data.date AND d.team = team_data.team
  ),
  cte_two AS (
    SELECT d.id,
      d.team,
      d.date,
      cte_one.opp_fga,
      cte_one.opp_fg,
      cte_one.team_blks,
      cte_one.opp_tov,
      cte_one.team_stls,
      cte_one.team_pf,
      cte_one.opp_fta,
      cte_one.opp_ft,
      cte_one.DOR_pct,
      cte_one.opp_minutes,
      cte_one.team_possessions,
      cte_one.team_minutes, 
      cte_one.opp_pts,
      ((cte_one.DFG_pct * (1 - cte_one.DOR_pct)) / (cte_one.DFG_pct * (1 - cte_one.DOR_pct) + (1 - cte_one.DFG_pct) * cte_one.DOR_pct)) AS FMwt
    FROM nightly_player_totals d
    JOIN cte_one ON d.id = cte_one.id
  ),
  cte_three AS (
    SELECT d.id,
    cte_two.opp_minutes,
    cte_two.team_possessions,
    cte_two.team_minutes,
    cte_two.opp_pts,
    cte_two.opp_fta,
    cte_two.opp_ft,
    cte_two.opp_fg,
    (player_data.steals + player_data.blocks * cte_two.FMwt * (1 - 1.07 * cte_two.DOR_pct) + player_data.defensive_rebounds * (1 - cte_two.FMwt)) AS Stops1,
    ((((cte_two.opp_fga - cte_two.opp_fg - cte_two.team_blks) / cte_two.team_minutes) * cte_two.FMwt * (1 - 1.07 * cte_two.DOR_pct) + ((cte_two.opp_tov - cte_two.team_stls) /  cte_two.team_minutes)) * player_data.minutes_played + (player_data.personal_fouls /  cte_two.team_pf) * 0.4 * cte_two.opp_fta * POWER((1 - (cte_two.opp_ft / cte_two.opp_fta)),2)) AS Stops2
    FROM nightly_player_totals d
    JOIN cte_two ON d.id = cte_two.id
    JOIN player_data ON d.id = player_data.id
  ),
  cte_four AS (
    SELECT d.id,
    (cte_three.Stops1 + cte_three.Stops2) AS stops,
    (((cte_three.Stops1 + cte_three.Stops2) * cte_three.opp_minutes)/(cte_three.team_possessions * cte_three.team_minutes)) AS stop_pct,
    (100 * (cte_three.opp_pts / cte_three.team_possessions)) AS Team_Defensive_Rating,
    (cte_three.opp_pts/ (cte_three.opp_fg + (1 - POWER((1 - (cte_three.opp_ft / cte_three.opp_fta)),2)) * cte_three.opp_fta *0.4)) AS D_Pts_per_ScPoss
    FROM nightly_player_totals d
    JOIN cte_three ON d.id = cte_three.id
  )
SELECT m.id,
  DATE_FORMAT(m.date, '%Y-%m-%d') AS date,
  m.team,
  m.opponent,
  m.player_name,
  cte_four.stops,
  cte_four.stop_pct,
  ROUND(CAST((cte_four.Team_Defensive_Rating) AS FLOAT),2) AS team_def_rating,
  (cte_four.Team_Defensive_Rating + 0.2 * (100 * cte_four.D_Pts_per_ScPoss * (1 - cte_four.stop_pct) - cte_four.Team_Defensive_Rating)) AS def_rating
  FROM nightly_player_totals m
  JOIN cte_four ON m.id = cte_four.id
);
-- INSERT IGNORE INTO adv_stats_player (id, date, team, opponent, player_name,
--   stops, stops_pct, team_def_rating, def_rating)
SELECT *
  FROM def_1
