# file_properties.sql


drop table if exists general.file_properties;


create table
general.file_properties
as
select
cf.repo_name as repo_name
, file
, '' as creator_name
, '' as creator_email
, min(cf.commit_timestamp) as min_commit_time
, max(cf.commit_timestamp) as max_commit_time
, min(cf.commit) as min_commit
, max(extension) as extension
, max(code_extension) as code_extension
, max(is_test) as is_test
, count(distinct cf.commit) as commits
, count(distinct if(parents = 1, cf.commit, null)) as non_merge_commits
, count(distinct case when cf.is_corrective  then cf.commit else null end) as corrective_commits
, 1.0*count(distinct if(cf.is_corrective, cf.commit, null))/count(distinct cf.commit) as corrective_rate
, general.bq_ccp_mle(1.0*count(distinct if(cf.is_corrective, cf.commit, null))/count(distinct cf.commit)) as ccp
, avg(if(not cf.is_corrective, non_test_files, null)) as avg_coupling_size
, avg(if(not cf.is_corrective, code_non_test_files, null)) as avg_coupling_code_size
, avg(if(not cf.is_corrective, if(non_test_files > 103 , 103 , non_test_files), null)) as avg_coupling_size_capped
, avg(if(not cf.is_corrective, if(code_non_test_files> 103 , 103 ,code_non_test_files), null)) as avg_coupling_code_size_capped
, avg(if(not cf.is_corrective, if(non_test_files > 103 , null , non_test_files), null)) as avg_coupling_size_cut
, avg(if(not cf.is_corrective, if(code_non_test_files> 103 , null ,code_non_test_files), null)) as avg_coupling_code_size_cut
, avg(if(not cf.is_corrective, if(code_non_test_files> 10 , null ,code_non_test_files), null)) as avg_coupling_code_size_cut10

, if(sum(if(files <= 103, files, null)) > 0
    , sum(if(files <= 103, files - non_test_files, null))/ sum(if(files <= 103, files, null))
    , null) as test_file_ratio_cut
, if(sum(if(code_files <= 103, code_files, null)) > 0
    , sum(if(code_files <= 103, code_files - code_non_test_files, null))/ sum(if(code_files <= 103, code_files, null))
    , null) as test_code_file_ratio_cut

, count(distinct cf.Author_email) as authors
, max(cf.Author_email) as Author_email # Meaningful only when authors=1
, min(ec.commit_month) as commit_month

, avg(if(same_date_as_prev, duration, null)) as same_day_duration_avg

, 0.0 as prev_touch_ago

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

drop table if exists general.file_first_commit;


create table
general.file_first_commit
as
select
fp.repo_name as repo_name
, fp.file as file
, min(fp.min_commit_time) as min_commit_time
, min(cf.author_name) as creator_name
, min(cf.author_email) as creator_email
, count(distinct commit) as commits # For uniqueness checking
, count(distinct cf.author_email) as authors # For uniqueness checking
from
general.file_properties as fp
join
general.commits_files as cf
on
fp.repo_name = cf.repo_name
and
fp.file = cf.file
and
fp.min_commit_time = cf.commit_timestamp
group by
fp.repo_name
, fp.file
;


update general.file_properties as fp
set creator_name = ffc.creator_name, creator_email = ffc.creator_email
from
general.file_first_commit as ffc
where
fp.repo_name = ffc.repo_name
and
fp.file = ffc.file
and
fp.min_commit_time = ffc.min_commit_time
and
ffc.authors = 1 # For uniqueness checking
;

drop table if exists general.file_first_commit;


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
, 1.0*count(distinct if(cf.is_corrective, cf.commit, null))/count(distinct cf.commit) as corrective_rate
, general.bq_ccp_mle(1.0*count(distinct if(cf.is_corrective, cf.commit, null))/count(distinct cf.commit)) as ccp
, avg(if(not cf.is_corrective, non_test_files, null)) as avg_coupling_size
, avg(if(not cf.is_corrective, code_non_test_files, null)) as avg_coupling_code_size
, avg(if(not cf.is_corrective, if(non_test_files > 103 , 103 , non_test_files), null)) as avg_coupling_size_capped
, avg(if(not cf.is_corrective, if(code_non_test_files> 103 , 103 ,code_non_test_files), null)) as avg_coupling_code_size_capped
, avg(if(not cf.is_corrective, if(non_test_files > 103 , null , non_test_files), null)) as avg_coupling_size_cut
, avg(if(not cf.is_corrective, if(code_non_test_files> 103 , null ,code_non_test_files), null)) as avg_coupling_code_size_cut
, avg(if(not cf.is_corrective, if(code_non_test_files> 10 , null ,code_non_test_files), null)) as avg_coupling_code_size_cut10

, if(sum(if(files <= 103, files, null)) > 0
    , sum(if(files <= 103, files - non_test_files, null))/ sum(if(files <= 103, files, null))
    , null) as test_file_ratio_cut
, if(sum(if(code_files <= 103, code_files, null)) > 0
    , sum(if(code_files <= 103, code_files - code_non_test_files, null))/ sum(if(code_files <= 103, code_files, null))
    , null) as test_code_file_ratio_cut


, count(distinct cf.Author_email) as authors
, max(cf.Author_email) as Author_email # Meaningful only when authors=1
, min(ec.commit_month) as commit_month
, avg(if(same_date_as_prev, duration, null)) as same_day_duration_avg

, 0.0 as prev_touch_ago

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
