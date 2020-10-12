
drop table if exists general.commits_file_prev_timestamp;

create table
general.commits_file_prev_timestamp
as
select
cur.repo_name as repo_name
, cur.commit as commit
, cur.file as file
, max(cur.author_email) as author_email
, max(cur.commit_timestamp) as commit_timestamp
, max(prev.commit_timestamp) as prev_timestamp
from
general.commits_files as cur
left join
general.commits_files as prev
on
cur.repo_name = prev.repo_name
and
cur.file = prev.file
and
cur.commit_timestamp > prev.commit_timestamp
group by
cur.repo_name
, cur.commit
, cur.file
;


drop table if exists general.commits_file_with_prev;

create table
general.commits_file_with_prev
as
select
cur.repo_name as repo_name
, cur.commit as commit
, cur.file as file
, max(cur.commit_timestamp) as commit_timestamp
, max(cur.prev_timestamp) as prev_timestamp
, max(prev.commit) as prev_commit # The max is extra safety for the case of two commits in the same time
from
general.commits_file_prev_timestamp as cur
left join
general.commits_file_prev_timestamp as prev
on
cur.repo_name = prev.repo_name
and
cur.file = prev.file
and
cur.prev_timestamp = prev.commit_timestamp
group by
cur.repo_name
, cur.commit
, cur.file
;


drop table if exists general.commits_file_with_prev_touch;

create table
general.commits_file_with_prev_touch
as
select
main.*
, TIMESTAMP_DIFF(commit_timestamp, prev_timestamp, minute) as prev_touch_ago
, date(commit_timestamp) = date(prev_timestamp) as same_date_as_prev
from
general.commits_file_with_prev as main
;
