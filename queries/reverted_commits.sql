# General - reverted_commits.sql

drop table if exists general_large.reverted_commits_raw;


create table
general_large.reverted_commits_raw
as
select
repo_name
, commit as reverting_commit
, commit_timestamp as reverting_commit_timestamp
, substr(REGEXP_EXTRACT(message, 'This reverts commit [0-9a-f]{5,40}')
  , length('This reverts commit ') + 1) as reverted_commit
from
general_large.enhanced_commits
where
regexp_contains(message, 'This reverts commit [0-9a-f]{5,40}')
;

drop table if exists general_large.reverted_commits;


create table
general_large.reverted_commits
as
select
raw.*
, reverted.commit_timestamp as reverted_commit_timestamp
, TIMESTAMP_DIFF(reverting_commit_timestamp, reverted.commit_timestamp, minute) as minutes_to_revert
from
general_large.reverted_commits_raw as raw
join
general_large.enhanced_commits as reverted
on
raw.reverted_commit = reverted.commit
and
raw.repo_name = reverted.repo_name
;


drop table if exists general_large.reverted_commits_raw;
