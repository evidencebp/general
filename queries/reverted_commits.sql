drop table if exists general.reverted_commits_raw;


create table
general.reverted_commits_raw
as
select
repo_name
, commit as reverting_commit
, commit_timestamp as reverting_commit_timestamp
, substr(REGEXP_EXTRACT(message, 'This reverts commit [0-9a-f]{5,40}')
  , length('This reverts commit ') + 1) as reverted_commit
from
general.enhanced_commits
where
regexp_contains(message, 'This reverts commit [0-9a-f]{5,40}')
;

drop table if exists general.reverted_commits;


create table
general.reverted_commits
as
select
raw.*
, reverted.commit_timestamp as reverted_commit_timestamp
, TIMESTAMP_DIFF(reverting_commit_timestamp, reverted.commit_timestamp, minute) as minutes_to_revert
from
general.reverted_commits_raw as raw
join
general.enhanced_commits as reverted
on
raw.reverted_commit = reverted.commit
and
raw.repo_name = reverted.repo_name
;


# Star at repo level
Select
stars >= 7481 as is_popular
, count(*) as projects
, avg(reverted.minutes_to_revert) as minutes_to_revert
, avg(reverted.minutes_to_revert/60/24) as days_to_revert
from
general.repo_properties as r
join
general.reverted_commits as reverted
on
r.repo_name = reverted.repo_name
group by
is_popular
order by
is_popular
;

# Tests at repo level
select
tests_presence <= 0.01 as lacking_tests
, count(*) as projects
, avg(reverted.minutes_to_revert) as minutes_to_revert
, avg(reverted.minutes_to_revert/60/24) as days_to_revert
from
general.repo_properties as r
join
general.reverted_commits as reverted
on
r.repo_name = reverted.repo_name
group by
lacking_tests
order by
lacking_tests
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
