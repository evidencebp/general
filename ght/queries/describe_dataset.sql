select
count(distinct repo_name) as repos
, count(distinct if(issues > 0, repo_name, null)) as repos_with_issues
, count(distinct if(pull_requests > 0, repo_name, null)) as repos_with_pull_requests
from
general_ght.repo_profile
;