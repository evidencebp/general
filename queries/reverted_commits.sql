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


drop table if exists general.repo_days_commits;

create table
general.repo_days_commits
select
repo_name
, date(commit_timestamp) as day
, count(distinct commit) as commits
from
general.enhanced_commits
group by
repo_name
, day
;


drop table if exists general.repo_days_commits;

create table
general.repo_days_commits
as
select
repo_name
, date(commit_timestamp) as day
, count(distinct commit) as commits
from
general.enhanced_commits
group by
repo_name
, day
;

drop table if exists general.repo_days_commits_avg;

create table
general.repo_days_commits_avg
as
select
repo_name
, avg(commits) as commits
from
general.repo_days_commits
group by
repo_name
;


Select
round(rdc.commits/rdca.commits) as f
, count(distinct ec.commit) as commits
, count(distinct rc.reverted_commit) as reverted_commits
, 1.0*count(distinct rc.reverted_commit)/count(distinct ec.commit) as reverted_commits_ratio
from
general.repo_days_commits as rdc
join
general.repo_days_commits_avg as rdca
on
rdc.repo_name = rdca.repo_name
join
general.enhanced_commits as ec
on
rdc.repo_name = ec.repo_name
and
rdc.day = date(ec.commit_timestamp)
left join
general.reverted_commits as rc
on
rc.repo_name = ec.repo_name
and
rc.reverted_commit = ec.commit
where
rdca.commits > 10
group by
f
order by
f
;

drop table if exists general.repo_days_commits;
drop table if exists general.repo_days_commits_avg;
