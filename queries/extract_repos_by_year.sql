# Standard Sql
drop table if exists general.2022_above_50;

create table
general.2022_above_50
as
SELECT
  commit_repo_name as repo_name,
  COUNT(DISTINCT
    CASE
      WHEN extract(year from TIMESTAMP_SECONDS(committer.date.seconds)) <= 2022 THEN commit
      ELSE NULL END) AS commits,
  COUNT(DISTINCT
    CASE
      WHEN extract(year from TIMESTAMP_SECONDS(committer.date.seconds)) = 2022 THEN commit
      ELSE NULL END) AS commits_2022,
  COUNT(DISTINCT committer.email) AS commiters,
  MIN(TIMESTAMP_SECONDS(committer.date.seconds)) AS start_time,
  MAX(TIMESTAMP_SECONDS(committer.date.seconds)) AS end_time
FROM
  `bigquery-public-data.github_repos.commits`
  cross join  UNNEST(repo_name) as commit_repo_name
GROUP BY
  commit_repo_name
HAVING
#  COUNT(DISTINCT
#    CASE
#      WHEN extract(year from TIMESTAMP_SECONDS(committer.date.seconds)) = 2022 THEN commit
#      ELSE NULL END) >= 50
COUNT(DISTINCT
    CASE
      WHEN extract(year from TIMESTAMP_SECONDS(committer.date.seconds)) <= 2022 THEN commit
      ELSE NULL END) >= 50
;