SELECT t1.date,
  t1.team,
  t1.opponent,
  SUM(t1.team_fg) AS team_fg,
  SUM(t1.team_fga) AS team_fga,
  SUM(t1.team_fta) AS team_fta,
  SUM(t1.team_orb) AS team_orb,
  SUM(t1.team_drb) AS team_drb,
  SUM(t1.team_tov) AS team_tov,
  SUM(t2.team_fg) AS opp_fg,
  SUM(t2.team_fga) AS opp_fga,
  SUM(t2.team_fta) AS opp_fta,
  SUM(t2.team_orb) AS opp_orb,
  SUM(t2.team_drb) AS opp_drb,
  SUM(t2.team_tov) AS opp_tov,
  (
    SUM(t1.team_fga) + 0.4 * SUM(t1.team_fta) - 1.07 * (
      SUM(t1.team_orb) / (SUM(t1.team_orb) + SUM(t2.team_drb))
    ) * (SUM(t1.team_fga) - SUM(t1.team_fg)) + SUM(t1.team_tov)
  ) AS team_possessions,
  (
    SUM(t2.team_fga) + 0.4 * SUM(t2.team_fta) - 1.07 * (
      SUM(t2.team_orb) / (SUM(t2.team_orb) + SUM(t1.team_drb))
    ) * (SUM(t2.team_fga) - SUM(t2.team_fg)) + SUM(t2.team_tov)
  ) AS opp_possessions
FROM (
    SELECT date,
      team,
      opponent,
      SUM(field_goals + three_point_field_goals) AS team_fg,
      SUM(
        field_goal_attempts + three_point_field_goal_attempts
      ) AS team_fga,
      SUM(free_throw_attempts) AS team_fta,
      SUM(offensive_rebounds) AS team_orb,
      SUM(defensive_rebounds) AS team_drb,
      SUM(turnovers) AS team_tov
    FROM nightly_player_totals
    GROUP BY date,
      team,
      opponent
  ) AS t1
  JOIN (
    SELECT date,
      team,
      opponent,
      SUM(field_goals + three_point_field_goals) AS team_fg,
      SUM(
        field_goal_attempts + three_point_field_goal_attempts
      ) AS team_fga,
      SUM(free_throw_attempts) AS team_fta,
      SUM(offensive_rebounds) AS team_orb,
      SUM(defensive_rebounds) AS team_drb,
      SUM(turnovers) AS team_tov
    FROM nightly_player_totals
    GROUP BY date,
      team,
      opponent
  ) AS t2 ON t1.date = t2.date
  AND t1.opponent = t2.team
GROUP BY t1.date,
  t1.team,
  t1.opponent;