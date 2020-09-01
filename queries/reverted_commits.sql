drop table if exists general.reverted_commits;


create table
general.reverted_commits
as
select
repo_name
, commit as reverting_commit
, substr(REGEXP_EXTRACT(message, 'This reverts commit [0-9a-f]{5,40}')
  , length('This reverts commit ') + 1) as reverted_commit
from
general.enhanced_commits
where
regexp_contains(message, 'This reverts commit [0-9a-f]{5,40}')
;


Select
extract(DAYOFWEEK from commit_timestamp) as f
, count(distinct ec.commit) as commits
, count(distinct rc.reverted_commit) as reverted_commits
, 1.0*count(distinct rc.reverted_commit)/count(distinct ec.commit) as reverted_commits_ratio
from
general.enhanced_commits as ec
left join
general.reverted_commits as rc
on
rc.repo_name = ec.repo_name
and
rc.reverted_commit = ec.commit
group by
f
order by
f
;
