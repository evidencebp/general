# Legacy
# Into 2020_above_50
SELECT
  repo_name,
  COUNT(DISTINCT commit) AS commits,
  COUNT(DISTINCT
    CASE
      WHEN YEAR(USEC_TO_TIMESTAMP(committer.date.seconds*1000000)) = 2020 THEN commit
      ELSE NULL END) AS commits_2020,
  COUNT(DISTINCT committer.email) AS commiters,
  MIN(USEC_TO_TIMESTAMP(committer.date.seconds*1000000)) AS start_time,
  MAX(USEC_TO_TIMESTAMP(committer.date.seconds*1000000)) AS end_time
FROM
  [bigquery-public-data:github_repos.commits]
GROUP BY
  Repo_name
HAVING
  COUNT(DISTINCT
    CASE
      WHEN YEAR(USEC_TO_TIMESTAMP(committer.date.seconds*1000000)) = 2020 THEN commit
      ELSE NULL END) >= 49
;

# Standard Sql
drop table if exists general.2020_above_50;

create table
general.2020_above_50
as
SELECT
  commit_repo_name as repo_name,
  COUNT(DISTINCT commit) AS commits,
  COUNT(DISTINCT
    CASE
      WHEN extract(year from TIMESTAMP_SECONDS(committer.date.seconds)) = 2020 THEN commit
      ELSE NULL END) AS commits_2020,
  COUNT(DISTINCT committer.email) AS commiters,
  MIN(TIMESTAMP_SECONDS(committer.date.seconds)) AS start_time,
  MAX(TIMESTAMP_SECONDS(committer.date.seconds)) AS end_time
FROM
  `bigquery-public-data.github_repos.commits`
  cross join  UNNEST(repo_name) as commit_repo_name
GROUP BY
  commit_repo_name
HAVING
  COUNT(DISTINCT
    CASE
      WHEN extract(year from TIMESTAMP_SECONDS(committer.date.seconds)) = 2020 THEN commit
      ELSE NULL END) >= 49
;