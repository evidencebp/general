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
, avg(if(not is_corrective and parents = 1, non_test_files, null)) as avg_coupling_size
, avg(if(not is_corrective and parents = 1, code_non_test_files, null)) as avg_coupling_code_size
, avg(if(not is_corrective and parents = 1, if(non_test_files > 103 , 103 , non_test_files), null)) as avg_coupling_size_capped
, avg(if(not is_corrective and parents = 1, if(code_non_test_files> 103 , 103 ,code_non_test_files), null)) as avg_coupling_code_size_capped
, avg(if(not is_corrective and parents = 1, if(non_test_files > 103 , null , non_test_files), null)) as avg_coupling_size_cut
, avg(if(not is_corrective and parents = 1, if(code_non_test_files> 103 , null ,code_non_test_files), null)) as avg_coupling_code_size_cut
, count(distinct Author_email) as authors
, max(Author_email) as Author_email # Meaningful only when authors=1
, avg(if(same_date_as_prev, duration, null)) as same_date_duration
, min(ec.commit_month) as commit_month
from
general.enhanced_commits as ec
group by
repo_name
, year
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
, avg(if(not is_corrective and parents = 1, non_test_files, null)) as avg_coupling_size
, avg(if(not is_corrective and parents = 1, code_non_test_files, null)) as avg_coupling_code_size
, avg(if(not is_corrective and parents = 1, if(non_test_files > 103 , 103 , non_test_files), null)) as avg_coupling_size_capped
, avg(if(not is_corrective and parents = 1, if(code_non_test_files> 103 , 103 ,code_non_test_files), null)) as avg_coupling_code_size_capped
, avg(if(not is_corrective and parents = 1, if(non_test_files > 103 , null , non_test_files), null)) as avg_coupling_size_cut
, avg(if(not is_corrective and parents = 1, if(code_non_test_files> 103 , null ,code_non_test_files), null)) as avg_coupling_code_size_cut
, count(distinct Author_email) as authors
, max(Author_email) as Author_email # Meaningful only when authors=1
, avg(if(same_date_as_prev, duration, null)) as same_date_duration
, min(ec.commit_month) as commit_month
from
general.enhanced_commits as ec
group by
repo_name
;


# into general_repo_properties_2019.csv
select *
from
general.repo_properties_per_year
where
year = 2019
and
commits > 10
order by
repo_name
;
