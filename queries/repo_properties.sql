drop table if exists general.repo_properties_per_year;


create table
general.repo_properties_per_year
as
select
repo_name as repo_name
, extract( year from commit_month) as year
, min(commit_timestamp) as min_commit_time
, max(commit_timestamp) as max_commit_time
, min(commit) as min_commit
, max(commit) as max_commit
, count(distinct commit) as commits
, count(distinct if(parents = 1, commit, null)) as non_merge_commits
, count(distinct case when is_corrective  then commit else null end) as corrective_commits
, if(count(distinct if(parents = 1, commit, null)) > 0
    ,1.0*count(distinct case when is_corrective and parents = 1 then commit else null end)/
        count(distinct if(parents = 1, commit, null))
    , null) as corrective_rate
, if(count(distinct if(parents = 1, commit, null)) > 0
    , 1.253*count(distinct case when is_corrective  and parents = 1 then commit else null end)/
        count(distinct if(parents = 1, commit, null)) -0.053
    , null ) as ccp

, 1.695*count(distinct case when is_refactor  then commit else null end)/count(distinct commit) -0.034 as refactor_mle

, avg(if(not is_corrective and parents = 1, non_test_files, null)) as avg_coupling_size
, avg(if(not is_corrective and parents = 1, code_non_test_files, null)) as avg_coupling_code_size
, avg(if(not is_corrective and parents = 1, if(non_test_files > 103 , 103 , non_test_files), null)) as avg_coupling_size_capped
, avg(if(not is_corrective and parents = 1, if(code_non_test_files> 103 , 103 ,code_non_test_files), null)) as avg_coupling_code_size_capped
, avg(if(not is_corrective and parents = 1, if(non_test_files > 103 , null , non_test_files), null)) as avg_coupling_size_cut
, avg(if(not is_corrective and parents = 1, if(code_non_test_files> 103 , null ,code_non_test_files), null)) as avg_coupling_code_size_cut

, count(distinct Author_email) as authors
, max(Author_email) as Author_email # Meaningful only when authors=1
, avg(if(same_date_as_prev, duration, null)) as same_day_duration_avg

, 0 as files_edited

, 0 as files_created
, 0.0 as files_created_ccp
, 0.0 as tests_presence

# Commit message linguistic characteristic (e.g., message length)
, 1.0*count(distinct if(REGEXP_CONTAINS(message,'\\n'), commit, null))/ count(distinct commit)
as multiline_message_ratio
, avg(length(message)) as message_length_avg

# Duration
, avg(case when same_date_as_prev then duration else null end) as same_date_duration_avg
, count(distinct case when same_date_as_prev then commit else null end) as same_date_commits

, TIMESTAMP_DIFF(max(ec.commit_timestamp), min(ec.commit_timestamp), day) as commit_period
, count(distinct date(ec.commit_timestamp)) as commit_days

, if(count(distinct date(ec.commit_timestamp)) > 0
 , 1.0*count(distinct commit)/count(distinct date(ec.commit_timestamp))
 , 1.0*count(distinct commit))as commits_per_day

, count(distinct extract(week from date(ec.commit_timestamp))) as commit_weeks
, count(distinct extract(month from date(ec.commit_timestamp))) as commit_months
, count(distinct extract(dayofweek from date(ec.commit_timestamp))) as commit_days_of_week
, count(distinct extract(hour from ec.commit_timestamp)) as commit_hours # Should see how diverse are the hours

# Density
, count(distinct ec.commit_timestamp)/(1+TIMESTAMP_DIFF(max(ec.commit_timestamp), min(ec.commit_timestamp), day)) as commit_day_density

, 0.0 as survival_avg
, 0.0 as above_year_prob

# Productivity metrics
, 0.0 as commits_per_developer
, 0 as involved_developers
, 0 as involved_developers_commits
, 0.0 as commits_per_involved_developer
, 0 as developer_capped_commits
, 0.0 as capped_commits_per_developer
, 0 as involved_developers_capped_commits
, 0.0 as capped_commits_per_involved_developer

from
general.enhanced_commits as ec
group by
repo_name
, year
;



drop table if exists general.created_files_by_repo_by_year;

create table
general.created_files_by_repo_by_year
as
select
repo_name
, extract(year from min_commit_time) as year
, count(distinct file) as files
, 1.253*sum(corrective_commits)/sum(commits) -0.053 as ccp
from
general.file_properties
group by
repo_name
, year
;

update general.repo_properties_per_year as dp
set files_created = acf.files, files_created_ccp = acf.ccp
from
general.created_files_by_repo_by_year as acf
where
dp.repo_name = acf.repo_name
and
dp.year = acf.year
;

drop table if exists general.created_files_by_repo_by_year;



drop table if exists general.edited_files_by_year;

create table
general.edited_files_by_year
as
select
 repo_name
, extract(year from commit_timestamp) as year
, count(distinct concat(repo_name, file)) as files
, 1.253*count(distinct if(is_corrective, commit, null))/count(distinct commit) -0.053 as ccp
, sum(if(is_test, 1,0))/count(*)  as tests_presence
from
general.commits_files
group by
repo_name
, year
;


update general.repo_properties_per_year as dp
set files_edited = aef.files, tests_presence = round(aef.tests_presence, 2)
from
general.edited_files_by_year as aef
where
dp.repo_name = aef.repo_name
and
dp.year = aef.year
;

drop table if exists general.edited_files_by_year;



drop table if exists general.files_survival_by_year;

create table
general.files_survival_by_year
as
select
 repo_name
, extract(year from min_commit_time) as year
, avg(TIMESTAMP_DIFF(max_commit_time, min_commit_time, DAY)) as survival_avg
, avg(if(TIMESTAMP_DIFF(max_commit_time, min_commit_time, DAY) > 365, 1, 0)) as above_year_prob
from
general.file_properties
where
extract(year from min_commit_time)  < extract(year from CURRENT_DATE())
group by
repo_name
, extract(year from min_commit_time)
;

update general.repo_properties_per_year as rp
set survival_avg = fs.survival_avg
, above_year_prob = fs.above_year_prob
from
general.files_survival_by_year as fs
where
rp.repo_name = fs.repo_name
and
rp.year = fs.year
;

drop table if exists general.files_survival_by_year;

update general.repo_properties_per_year as rp
set survival_avg = Null
, above_year_prob = Null
where
survival_avg = 0.0
and
above_year_prob = 0.0
;

drop table if exists general.repo_properties;


create table
general.repo_properties
as
select
repo_name as repo_name
, min(commit_timestamp) as min_commit_time
, max(commit_timestamp) as max_commit_time
, min(commit) as min_commit
, max(commit) as max_commit
, count(distinct commit) as commits
, count(distinct if(parents = 1, commit, null)) as non_merge_commits
, count(distinct case when is_corrective  then commit else null end) as corrective_commits
, if(count(distinct if(parents = 1, commit, null)) > 0
    ,1.0*count(distinct case when is_corrective and parents = 1 then commit else null end)/
        count(distinct if(parents = 1, commit, null))
    , null) as corrective_rate
, if(count(distinct if(parents = 1, commit, null)) > 0
    , 1.253*count(distinct case when is_corrective  and parents = 1 then commit else null end)/
        count(distinct if(parents = 1, commit, null)) -0.053
    , null ) as ccp

 , 1.695*count(distinct case when is_refactor  then commit else null end)/count(distinct commit) -0.034 as refactor_mle

, avg(if(not is_corrective and parents = 1, non_test_files, null)) as avg_coupling_size
, avg(if(not is_corrective and parents = 1, code_non_test_files, null)) as avg_coupling_code_size
, avg(if(not is_corrective and parents = 1, if(non_test_files > 103 , 103 , non_test_files), null)) as avg_coupling_size_capped
, avg(if(not is_corrective and parents = 1, if(code_non_test_files> 103 , 103 ,code_non_test_files), null)) as avg_coupling_code_size_capped
, avg(if(not is_corrective and parents = 1, if(non_test_files > 103 , null , non_test_files), null)) as avg_coupling_size_cut
, avg(if(not is_corrective and parents = 1, if(code_non_test_files> 103 , null ,code_non_test_files), null)) as avg_coupling_code_size_cut

, count(distinct Author_email) as authors
, max(Author_email) as Author_email # Meaningful only when authors=1
, avg(if(same_date_as_prev, duration, null)) as same_day_duration_avg

, 0 as files_edited # edited/created are similar in the repo level

, 0 as files_created
, 0.0 as files_created_ccp # The is the ccp, when working in the repo level
, 0.0 as tests_presence

# Commit message linguistic characteristic (e.g., message length)
, 1.0*count(distinct if(REGEXP_CONTAINS(message,'\\n'), commit, null))/ count(distinct commit)
as multiline_message_ratio
, avg(length(message)) as message_length_avg

# Duration
, avg(case when same_date_as_prev then duration else null end) as same_date_duration_avg
, count(distinct case when same_date_as_prev then commit else null end) as same_date_commits

, TIMESTAMP_DIFF(max(ec.commit_timestamp), min(ec.commit_timestamp), day) as commit_period
, count(distinct date(ec.commit_timestamp)) as commit_days

, if(count(distinct date(ec.commit_timestamp)) > 0
 , 1.0*count(distinct commit)/count(distinct date(ec.commit_timestamp))
 , 1.0*count(distinct commit))as commits_per_day

, count(distinct extract(week from date(ec.commit_timestamp))) as commit_weeks
, count(distinct extract(month from date(ec.commit_timestamp))) as commit_months
, count(distinct extract(dayofweek from date(ec.commit_timestamp))) as commit_days_of_week
, count(distinct extract(hour from ec.commit_timestamp)) as commit_hours # Should see how diverse are the hours

# Density
, count(distinct ec.commit_timestamp)/(1+TIMESTAMP_DIFF(max(ec.commit_timestamp), min(ec.commit_timestamp), day)) as commit_day_density

, 0.0 as survival_avg
, 0.0 as above_year_prob

# Productivity metrics
, 0.0 as commits_per_developer
, 0 as involved_developers
, 0 as involved_developers_commits
, 0.0 as commits_per_involved_developer
, 0 as developer_capped_commits
, 0.0 as capped_commits_per_developer
, 0 as involved_developers_capped_commits
, 0.0 as capped_commits_per_involved_developer

from
general.enhanced_commits as ec
group by
repo_name
;


drop table if exists general.edited_files;

create table
general.edited_files
as
select
 repo_name
, count(distinct concat(repo_name, file)) as files
, 1.253*count(distinct if(is_corrective, commit, null))/count(distinct commit) -0.053 as ccp
, sum(if(is_test, 1,0))/count(*)  as tests_presence
from
general.commits_files
group by
repo_name
;


update general.repo_properties as dp
set files_edited = aef.files
, files_created = aef.files
, tests_presence = round(aef.tests_presence, 2)
from
general.edited_files as aef
where
dp.repo_name = aef.repo_name
;

drop table if exists general.edited_files;


drop table if exists general.files_survival;

create table
general.files_survival
as
select
 repo_name
, avg(TIMESTAMP_DIFF(max_commit_time, min_commit_time, DAY)) as survival_avg
, avg(if(TIMESTAMP_DIFF(max_commit_time, min_commit_time, DAY) > 365, 1, 0)) as above_year_prob
from
general.file_properties
where
extract(year from min_commit_time)  < extract(year from CURRENT_DATE())
group by
repo_name
;

update general.repo_properties as rp
set survival_avg = fs.survival_avg
, above_year_prob = fs.above_year_prob
from
general.files_survival as fs
where
rp.repo_name = fs.repo_name
;

drop table if exists general.files_survival;
