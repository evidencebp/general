# commit_file_prev_touch.sql

drop table if exists general.commits_files_prev_timestamp;

create table
general.commits_files_prev_timestamp
as
select
cur.repo_name as repo_name
, cur.commit as commit
, cur.file as file
, max(cur.author_email) as author_email
, count(distinct cur.author_email) as authors
, max(cur.is_corrective) as is_corrective
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
and not cur.is_test
and cur.code_extension #Running only on functional code
group by
cur.repo_name
, cur.commit
, cur.file
;


drop table if exists general.commits_files_with_prev;

create table
general.commits_files_with_prev
as
select
cur.repo_name as repo_name
, cur.commit as commit
, cur.file as file
, max(cur.author_email) as author_email
, count(distinct cur.author_email) as authors
, max(cur.is_corrective) as is_corrective
, max(cur.commit_timestamp) as commit_timestamp
, max(cur.prev_timestamp) as prev_timestamp
, max(prev.commit) as prev_commit # The max is extra safety for the case of two commits in the same time
from
general.commits_files_prev_timestamp as cur
left join
general.commits_files_prev_timestamp as prev
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


drop table if exists general.commits_files_with_prev_touch;

create table
general.commits_files_with_prev_touch
as
select
main.*
, TIMESTAMP_DIFF(commit_timestamp, prev_timestamp, minute) as prev_touch_ago
, date(commit_timestamp) = date(prev_timestamp) as same_date_as_prev
from
general.commits_files_with_prev as main
;


drop table if exists general.file_prev_touch_stats;

create table
general.file_prev_touch_stats
as
select
repo_name
, file
, avg(prev_touch_ago) as prev_touch_ago
, avg(if(is_corrective, prev_touch_ago, null)) as bug_prev_touch_ago
from
general.commits_files_with_prev_touch
group by
repo_name
, file
;

update general.file_properties as rp
set
prev_touch_ago = aux.prev_touch_ago
, bug_prev_touch_ago = aux.bug_prev_touch_ago
from
general.file_prev_touch_stats as aux
where
rp.repo_name = aux.repo_name
and
rp.file = aux.file
;



drop table if exists general.file_prev_touch_stats_per_year;

create table
general.file_prev_touch_stats_per_year
as
select
repo_name
, file
, extract(year from commit_timestamp) as year
, avg(prev_touch_ago) as prev_touch_ago
, avg(if(is_corrective, prev_touch_ago, null)) as bug_prev_touch_ago
from
general.commits_files_with_prev_touch
group by
repo_name
, file
, extract(year from commit_timestamp)
;

update general.file_properties_per_year as rp
set
prev_touch_ago = aux.prev_touch_ago
, bug_prev_touch_ago = aux.bug_prev_touch_ago
from
general.file_prev_touch_stats_per_year as aux
where
rp.repo_name = aux.repo_name
and
rp.file = aux.file
and
rp.year = aux.year
;


drop table if exists general.commit_prev_touch;

create table
general.commit_prev_touch
as
select
repo_name
, commit
, max(author_email) as author_email
, max(commit_timestamp) as commit_timestamp
, max(prev_touch_ago) as prev_touch_ago
, avg(if(is_corrective, prev_touch_ago, null)) as bug_prev_touch_ago
from
general.commits_files_with_prev_touch
group by
repo_name
, commit
;

# Repo
drop table if exists general.repo_prev_touch;

create table
general.repo_prev_touch
as
select
repo_name
, avg(prev_touch_ago) as prev_touch_ago
, avg(if(is_corrective, prev_touch_ago, null)) as bug_prev_touch_ago
from
general.commits_files_with_prev_touch
group by
repo_name
;

update general.repo_properties as rp
set
prev_touch_ago = aux.prev_touch_ago
, bug_prev_touch_ago = aux.bug_prev_touch_ago
from
general.repo_prev_touch as aux
where
rp.repo_name = aux.repo_name
;

update general.repo_properties as rp
set prev_touch_ago = null
where
prev_touch_ago = 0.0
;

update general.repo_properties as rp
set bug_prev_touch_ago = null
where
bug_prev_touch_ago = 0.0
;

drop table if exists general.repo_prev_touch;

# Repo per year
drop table if exists general.repo_per_year_prev_touch;

create table
general.repo_per_year_prev_touch
as
select
repo_name
, extract(year from commit_timestamp) as year
, avg(prev_touch_ago) as prev_touch_ago
, avg(if(is_corrective, prev_touch_ago, null)) as bug_prev_touch_ago
from
general.commits_files_with_prev_touch
group by
repo_name
, extract(year from commit_timestamp)
;


update general.repo_properties_per_year as rpy
set
prev_touch_ago = aux.prev_touch_ago
, bug_prev_touch_ago = aux.bug_prev_touch_ago
from
general.repo_per_year_prev_touch as aux
where
rpy.repo_name = aux.repo_name
and
rpy.year = aux.year
;

update general.repo_properties_per_year as rp
set prev_touch_ago = null
where
prev_touch_ago = 0.0
;

update general.repo_properties_per_year as rp
set bug_prev_touch_ago = null
where
bug_prev_touch_ago = 0.0
;

# Developer
drop table if exists general.dev_prev_touch;

create table
general.dev_prev_touch
as
select
author_email
, avg(prev_touch_ago) as prev_touch_ago
, avg(if(is_corrective, prev_touch_ago, null)) as bug_prev_touch_ago
from
general.commits_files_with_prev_touch
where
authors = 1 # Files owned by the developer
group by
author_email
;


update general.developer_profile as t
set
prev_touch_ago = aux.prev_touch_ago
, bug_prev_touch_ago = aux.bug_prev_touch_ago
from
general.dev_prev_touch as aux
where
t.author_email = aux.author_email
;

update general.developer_profile as rp
set prev_touch_ago = null
where
prev_touch_ago = 0.0
;

update general.developer_profile as rp
set bug_prev_touch_ago = null
where
bug_prev_touch_ago = 0.0
;


drop table if exists general.dev_prev_touch;


# Developer per repo
drop table if exists general.dev_per_repo_prev_touch;

create table
general.dev_per_repo_prev_touch
as
select
author_email
, repo_name
, avg(prev_touch_ago) as prev_touch_ago
, avg(if(is_corrective, prev_touch_ago, null)) as bug_prev_touch_ago
from
general.commits_files_with_prev_touch
where
authors = 1 # Files owned by the developer
group by
author_email
, repo_name
;


update general.developer_per_repo_profile as t
set
prev_touch_ago = aux.prev_touch_ago
, bug_prev_touch_ago = aux.bug_prev_touch_ago
from
general.dev_per_repo_prev_touch as aux
where
t.author_email = aux.author_email
and
t.repo_name = aux.repo_name
;

update general.developer_per_repo_profile as rp
set prev_touch_ago = null
where
prev_touch_ago = 0.0
;

update general.developer_per_repo_profile as rp
set bug_prev_touch_ago = null
where
bug_prev_touch_ago = 0.0
;

drop table if exists general.dev_per_repo_prev_touch;

# Developer per repo per year
drop table if exists general.dev_per_repo_per_year_prev_touch;

create table
general.dev_per_repo_per_year_prev_touch
as
select
author_email
, repo_name
, extract(year from commit_timestamp) as year
, avg(prev_touch_ago) as prev_touch_ago
, avg(if(is_corrective, prev_touch_ago, null)) as bug_prev_touch_ago
from
general.commits_files_with_prev_touch
where
authors = 1 # Files owned by the developer
group by
author_email
, repo_name
, year
;


update general.developer_per_repo_profile_per_year as t
set
prev_touch_ago = aux.prev_touch_ago
, bug_prev_touch_ago = aux.bug_prev_touch_ago
from
general.dev_per_repo_per_year_prev_touch as aux
where
t.author_email = aux.author_email
and
t.repo_name = aux.repo_name
and
t.year = aux.year
;

update general.developer_per_repo_profile_per_year as rp
set prev_touch_ago = null
where
prev_touch_ago = 0.0
;

update general.developer_per_repo_profile_per_year as rp
set bug_prev_touch_ago = null
where
bug_prev_touch_ago = 0.0
;

drop table if exists general.dev_per_repo_per_year_prev_touch;




drop table if exists general.file_prev_touch_stats;
drop table if exists general.file_prev_touch_stats_per_year;

drop table if exists general.commits_files_prev_timestamp;
drop table if exists general.commits_files_with_prev;
drop table if exists general.commits_files_with_prev_touch;
