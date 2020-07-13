
drop table if exists general.commits_prev_timestamp;

create table
general.commits_prev_timestamp
as
select
cur.repo_name as repo_name
, cur.commit as commit
, max(cur.author_email) as author_email
, max(prev.commit_timestamp) as prev_timestamp
from
general.enhanced_commits as cur
left join
general.enhanced_commits as prev
on
cur.repo_name = prev.repo_name
and
cur.author_email = prev.author_email
and
cur.commit_timestamp > prev.commit_timestamp
group by
cur.repo_name
, cur.commit
;

drop table if exists general.commits_with_prev;

create table
general.commits_with_prev
as
select
cur.repo_name as repo_name
, cur.commit as commit
, max(cur.prev_timestamp) as prev_timestamp
, max(prev.commit) as prev_commit # The max is extra safety for the case of two commits in the same time
from
general.commits_prev_timestamp as cur
left join
general.enhanced_commits as prev
on
cur.repo_name = prev.repo_name
and
cur.author_email = prev.author_email
and
cur.prev_timestamp = prev.commit_timestamp
group by
cur.repo_name
, cur.commit
;


drop table if exists general.commits_with_duration;

create table
general.commits_with_duration
as
select
main.repo_name as repo_name
, main.commit as commit
, max(TIMESTAMP_DIFF(main.commit_timestamp, dur.prev_timestamp, minute)) as duration
, max(dur.prev_commit) as prev_commit
, max(dur.prev_timestamp) as prev_timestamp
, max(date(main.commit_timestamp) = date(dur.prev_timestamp)) as same_date_as_prev
from
general.enhanced_commits as main
join
general.commits_with_prev as dur
on
main.commit = dur.commit
and
main.repo_name = dur.repo_name
group by
main.repo_name
, main.commit
;

UPDATE  general.enhanced_commits AS ec
SET
ec.duration = cd.duration
, ec.prev_commit = cd.prev_commit
, ec.prev_timestamp = cd.prev_timestamp
, ec.same_date_as_prev = cd.same_date_as_prev
FROM general.commits_with_duration as cd
WHERE
ec.repo_name =  cd.repo_name
and
ec.commit =  cd.commit
;

drop table if exists general.commits_prev_timestamp;
drop table if exists general.commits_with_prev;
drop table if exists general.commits_with_duration;

