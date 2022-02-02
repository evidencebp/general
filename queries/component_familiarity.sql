# component_familiarity.sql
# TODO - check if year is handled well
# TODO - Check partitioning

drop table if exists general.component_familiarity_same_month;

create table
general.component_familiarity_same_month
partition by
commit_month
cluster by
repo_name, commit, file
as
select
cur_files.repo_name
, cur_files.commit as commit
, cur_files.file as file
, max(cast(FORMAT_DATE('%Y-%m-01', DATE(cur_files.commit_timestamp)) as date)) as  commit_month
, max(cur_files.Author_email) as Author_email
, count(distinct cur_files_history.commit) as prev_commits
, count(distinct cur_files_history.Author_email) as prev_authors
, max(cur_files_history.commit_timestamp	) as max_commit_timestamp
, max(if(cur_files_history.Author_email = cur_files.Author_email, cur_files_history.commit_timestamp, null))
as max_author_commit_timestamp
, count(distinct if(cur_files_history.Author_email = cur_files.Author_email
, cur_files_history.commit,null)) as author_commits
, min(TIMESTAMP_DIFF(cur_files.commit_timestamp, cur_files_history.commit_timestamp, minute)) as touched_minutes_ago
, min(TIMESTAMP_DIFF(cur_files.commit_timestamp
    , if(cur_files_history.Author_email = cur_files.Author_email, cur_files_history.commit_timestamp, null)
        , minute)) as touched_by_author_minutes_ago
from
general.commits_files as cur_files
left join
general.commits_files as cur_files_history
on
cur_files_history.commit_month = cur_files.commit_month
and
cur_files.repo_name = cur_files_history.repo_name
and
cur_files.file = cur_files_history.file
and
cur_files_history.commit_timestamp < cur_files.commit_timestamp
group by
repo_name
, cur_files.commit
, cur_files.file
;

drop table if exists general.component_familiarity_by_author_and_month;


create table
general.component_familiarity_by_author_and_month
partition by
commit_month
cluster by
repo_name, file, Author_email
as
select
repo_name
, file as file
, Author_email
, cast(FORMAT_DATE('%Y-%m-01', DATE(commit_timestamp)) as date) as  commit_month
, max(commit_timestamp) as max_commit_timestamp
, count(distinct commit) as commits
, count(distinct if(is_corrective, commit, null)) as corrective_commits
, 1.253*count(distinct if(is_corrective, commit, null))/count(distinct commit) -0.053 as ccp
from
general.commits_files
group by
repo_name
, file
, Author_email
, commit_month
;


drop table if exists general.component_familiarity_by_prev_months;

create table
general.component_familiarity_by_prev_months
partition by
commit_month
cluster by
repo_name, commit, file
as
select
cur_files.repo_name
, cur_files.commit as commit
, cur_files.file as file
, max(cur_files.Author_email) as Author_email
, max(cur_files.commit_timestamp) as commit_timestamp
, max(cd.duration) as duration
, max(cd.same_date_as_prev) as same_date_as_prev
, sum( cur_files_history.commits) as commits
, sum(corrective_commits) as corrective_commits
, count(distinct cur_files_history.Author_email) as prev_authors
, sum( if(cur_files_history.Author_email = cur_files.Author_email
, cur_files_history.commits,null)) as author_commits
, max(cur_files_history.max_commit_timestamp	) as max_commit_timestamp
, max(if(cur_files_history.Author_email = cur_files.Author_email, cur_files_history.max_commit_timestamp, null))
as max_author_commit_timestamp
, max(cast(FORMAT_DATE('%Y-%m-01', DATE(cur_files.commit_timestamp)) as date)) as  commit_month
from
general.commits_files as cur_files
join
general.enhanced_commits as cd
on
cur_files.repo_name = cd.repo_name
and
cur_files.commit = cd.commit
left join
general.component_familiarity_by_author_and_month as cur_files_history
on
cur_files_history.commit_month < cur_files.commit_month
and
cur_files.repo_name = cur_files_history.repo_name
and
cur_files.file = cur_files_history.file
and
cur_files_history.max_commit_timestamp < cur_files.commit_timestamp
group by
repo_name
, cur_files.commit
, cur_files.file
;



drop table if exists general.component_familiarity_joint;


create table
general.component_familiarity_joint
partition by
commit_month
cluster by
repo_name, commit, file
as
select
cur_files.repo_name
, cur_files.commit as commit
, cur_files.file as file
, count(distinct cur_files_history.commit) as same_month_prev_commits
, max(cur_files_history.commit_timestamp	) as same_month_max_commit_timestamp
, max(if(cur_files_history.Author_email = cur_files.Author_email, cur_files_history.commit_timestamp, null))
as same_month_max_author_commit_timestamp
, count(distinct if(cur_files_history.Author_email = cur_files.Author_email
, cur_files_history.commit,null)) as same_month_author_commits

, min(TIMESTAMP_DIFF(cur_files.commit_timestamp, cur_files_history.commit_timestamp, minute)) as same_month_touched_minutes_ago
, min(TIMESTAMP_DIFF(cur_files.commit_timestamp
    , if(cur_files_history.Author_email = cur_files.Author_email, cur_files_history.commit_timestamp, null)
        , minute)) as same_month_touched_by_author_minutes_ago

, max(cur_files.commit_timestamp) as prev_months_commit_timestamp
, max(cur_files.duration) as duration
, max(cur_files.same_date_as_prev) as same_date_as_prev
, max( cur_files.commits ) as prev_months_commits
, max(cur_files.corrective_commits) as prev_months_corrective_commits
, max( if(cur_files_history.Author_email = cur_files.Author_email
, cur_files.commits,null)) as prev_months_author_commits
, max(cur_files.max_commit_timestamp	) as prev_months_max_commit_timestamp
, max(if(cur_files_history.Author_email = cur_files.Author_email, cur_files.max_commit_timestamp, null))
as prev_months_max_author_commit_timestamp

, min(TIMESTAMP_DIFF(cur_files.commit_timestamp, cur_files.max_commit_timestamp, minute)) as prev_month_touched_minutes_ago
, min(TIMESTAMP_DIFF(cur_files.commit_timestamp
    , cur_files.max_author_commit_timestamp
        , minute)) as prev_month_touched_by_author_minutes_ago


, max(cast(FORMAT_DATE('%Y-%m-01', DATE(cur_files.commit_timestamp)) as date)) as  commit_month
from
general.component_familiarity_by_prev_months as cur_files
left join
general.commits_files as cur_files_history
on
cur_files_history.commit_month = cur_files.commit_month
and
cur_files.repo_name = cur_files_history.repo_name
and
cur_files.file = cur_files_history.file
and
cur_files_history.commit_timestamp < cur_files.commit_timestamp
group by
repo_name
, cur_files.commit
, cur_files.file
;


drop table if exists general.component_familiarity;


create table
general.component_familiarity
partition by
commit_month
cluster by
repo_name, commit, file
as
select
repo_name
, commit
, file
, commit_month
, same_date_as_prev
, duration
, ifnull(same_month_prev_commits, 0) + ifnull(prev_months_commits, 0) as prev_commits
, ifnull(same_month_author_commits, 0) + ifnull(prev_months_author_commits, 0) as prev_author_commits
, if(ifnull(same_month_prev_commits, 0) + ifnull(prev_months_commits, 0) > 0,
 1.0*(ifnull(same_month_author_commits, 0) + ifnull(prev_months_author_commits, 0))/(ifnull(same_month_prev_commits, 0) + ifnull(prev_months_commits, 0))
 , 0) as prev_author_commits_ratio
, greatest( same_month_max_commit_timestamp, prev_months_max_commit_timestamp) as prev_file_commit_timestamp
, greatest( same_month_max_author_commit_timestamp,  prev_months_max_author_commit_timestamp ) as prev_author_file_commit_timestamp
, least( same_month_touched_minutes_ago, prev_month_touched_minutes_ago ) as touched_minutes_ago
, least( same_month_touched_by_author_minutes_ago, prev_month_touched_by_author_minutes_ago ) as author_touched_minutes_ago
from
general.component_familiarity_joint
;


drop table if exists general.recent_component_familiarity_by_dev;

create table
general.recent_component_familiarity_by_dev
as
select
repo_name
, file
, Author_email
, count(distinct cur_files.commit) as commits
, min(cur_files.commit_timestamp) as min_commit_timestamp
, max(cur_files.commit_timestamp) as max_commit_timestamp
from
general.commits_files as cur_files
where
cur_files.commit_timestamp < TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL -30 DAY)
group by
repo_name
, file
, Author_email
;

drop table if exists general.recent_component_familiarity;

create table
general.recent_component_familiarity
as
select
repo_name
, file
, count(distinct Author_email) as authors
, max(Author_email) as Author_email # Meaningful only when authors=1
, count(distinct cur_files.commit) as commits
, min(cur_files.commit_timestamp) as min_commit_timestamp
, max(cur_files.commit_timestamp) as max_commit_timestamp
from
general.commits_files as cur_files
where
cur_files.commit_timestamp < TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL -30 DAY)
group by
repo_name
, file
;
