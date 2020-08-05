drop table if exists general.file_properties_per_year;


create table
general.file_properties_per_year
as
select
cf.repo_name as repo_name
, file
, extract( year from cf.commit_month) as year
, min(cf.commit_timestamp) as min_commit_time
, max(cf.commit_timestamp) as max_commit_time
, min(cf.commit) as min_commit
, max(extension) as extension
, max(code_extension) as code_extension
, max(is_test) as is_test
, count(distinct cf.commit) as commits
, count(distinct if(parents = 1, cf.commit, null)) as non_merge_commits
, count(distinct case when cf.is_corrective  then cf.commit else null end) as corrective_commits
, if(count(distinct if(parents = 1, cf.commit, null)) > 0
    ,1.0*count(distinct case when cf.is_corrective and parents = 1 then cf.commit else null end)/
        count(distinct if(parents = 1, cf.commit, null))
    , null) as corrective_rate
, if(count(distinct if(parents = 1, cf.commit, null)) > 0
    , 1.253*count(distinct case when cf.is_corrective  and parents = 1 then cf.commit else null end)/
        count(distinct if(parents = 1, cf.commit, null)) -0.053
    , null ) as ccp
, avg(if(not cf.is_corrective and parents = 1, non_test_files, null)) as avg_coupling_size
, avg(if(not cf.is_corrective and parents = 1, code_non_test_files, null)) as avg_coupling_code_size
, avg(if(not cf.is_corrective and parents = 1, if(non_test_files > 103 , 103 , non_test_files), null)) as avg_coupling_size_capped
, avg(if(not cf.is_corrective and parents = 1, if(code_non_test_files> 103 , 103 ,code_non_test_files), null)) as avg_coupling_code_size_capped
, avg(if(not cf.is_corrective and parents = 1, if(non_test_files > 103 , null , non_test_files), null)) as avg_coupling_size_cut
, avg(if(not cf.is_corrective and parents = 1, if(code_non_test_files> 103 , null ,code_non_test_files), null)) as avg_coupling_code_size_cut

, if(sum(if(files <= 103, files, null)) > 0
    , sum(if(files <= 103, files - non_test_files, null))/ sum(if(files <= 103, files, null))
    , null) as test_file_ratio_cut
, if(sum(if(code_files <= 103, code_files, null)) > 0
    , sum(if(code_files <= 103, code_files - code_non_test_files, null))/ sum(if(code_files <= 103, code_files, null))
    , null) as test_code_file_ratio_cut


, count(distinct cf.Author_email) as authors
, max(cf.Author_email) as Author_email # Meaningful only when authors=1
, min(ec.commit_month) as commit_month

, avg(if(ec.same_date_as_prev and parents = 1, duration, null)) as same_day_duration_avg

from
general.commits_files as cf
join
general.enhanced_commits as ec
on
cf.commit = ec.commit and cf.repo_name = ec.repo_name
and extract( year from cf.commit_month) =  extract( year from ec.commit_month)
group by
repo_name
, file
, year
;

drop table if exists general.file_properties;


create table
general.file_properties
as
select
cf.repo_name as repo_name
, file
, min(cf.commit_timestamp) as min_commit_time
, max(cf.commit_timestamp) as max_commit_time
, min(cf.commit) as min_commit
, max(extension) as extension
, max(code_extension) as code_extension
, max(is_test) as is_test
, count(distinct cf.commit) as commits
, count(distinct if(parents = 1, cf.commit, null)) as non_merge_commits
, count(distinct case when cf.is_corrective  then cf.commit else null end) as corrective_commits
, if(count(distinct if(parents = 1, cf.commit, null)) > 0
    ,1.0*count(distinct case when cf.is_corrective and parents = 1 then cf.commit else null end)/
        count(distinct if(parents = 1, cf.commit, null))
    , null) as corrective_rate
, if(count(distinct if(parents = 1, cf.commit, null)) > 0
    , 1.253*count(distinct case when cf.is_corrective  and parents = 1 then cf.commit else null end)/
        count(distinct if(parents = 1, cf.commit, null)) -0.053
    , null ) as ccp
, avg(if(not cf.is_corrective and parents = 1, non_test_files, null)) as avg_coupling_size
, avg(if(not cf.is_corrective and parents = 1, code_non_test_files, null)) as avg_coupling_code_size
, avg(if(not cf.is_corrective and parents = 1, if(non_test_files > 103 , 103 , non_test_files), null)) as avg_coupling_size_capped
, avg(if(not cf.is_corrective and parents = 1, if(code_non_test_files> 103 , 103 ,code_non_test_files), null)) as avg_coupling_code_size_capped
, avg(if(not cf.is_corrective and parents = 1, if(non_test_files > 103 , null , non_test_files), null)) as avg_coupling_size_cut
, avg(if(not cf.is_corrective and parents = 1, if(code_non_test_files> 103 , null ,code_non_test_files), null)) as avg_coupling_code_size_cut

, if(sum(if(files <= 103, files, null)) > 0
    , sum(if(files <= 103, files - non_test_files, null))/ sum(if(files <= 103, files, null))
    , null) as test_file_ratio_cut
, if(sum(if(code_files <= 103, code_files, null)) > 0
    , sum(if(code_files <= 103, code_files - code_non_test_files, null))/ sum(if(code_files <= 103, code_files, null))
    , null) as test_code_file_ratio_cut

, count(distinct cf.Author_email) as authors
, max(cf.Author_email) as Author_email # Meaningful only when authors=1
, min(ec.commit_month) as commit_month

, avg(if(ec.same_date_as_prev and parents = 1, duration, null)) as same_day_duration_avg

from
general.commits_files as cf
join
general.enhanced_commits as ec
on
cf.commit = ec.commit and cf.repo_name = ec.repo_name
group by
repo_name
, file
;
