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
      ELSE NULL END) > 99
;