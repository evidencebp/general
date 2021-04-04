# File properties per year
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
, avg(if(not cf.is_corrective, if(code_non_test_files> 10 , null ,code_non_test_files), null)) as avg_coupling_code_size_cut
#, avg(if(not cf.is_corrective, if(code_non_test_files> 10 , null ,code_non_test_files), null)) as avg_coupling_code_size_cut10

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

# Abstraction
, if (sum(if(ec.is_corrective, 1,0 )) > 0
, 1.0*sum(if( code_non_test_files = 1 and ec.is_corrective, 1,0 ))/sum(if(ec.is_corrective, 1,0 ))
, null)
as one_file_fix_rate
, if (sum(if(ec.is_refactor, 1,0 )) > 0
, 1.0*sum(if( code_non_test_files = 1 and ec.is_refactor, 1,0 ))/sum(if(ec.is_refactor, 1,0 ))
, null)
as one_file_refactor_rate

, if(sum(if((code_non_test_files = 1 and code_files = 2 ) or code_files=1, 1,0 )) > 0
    , 1.0*sum(if(code_files=1, 1,0 ))/sum(if((code_non_test_files = 1 and code_files = 2 ) or code_files=1, 1,0 ))
    , null)
as test_usage_rate

, if(sum(if(ec.is_refactor and ((code_non_test_files = 1 and code_files = 2 ) or code_files=1), 1,0 )) > 0
    , 1.0*sum(if(ec.is_refactor and code_files=1, 1,0 ))
        /sum(if(ec.is_refactor and ((code_non_test_files = 1 and code_files = 2 ) or code_files=1), 1,0 ))
    , null)
as test_usage_in_refactor_rate

, if(sum(if(ec.is_refactor, 1,0 )) > 0
    , 1.0*sum(if( code_non_test_files = code_files and ec.is_refactor, 1,0 ))/sum(if(ec.is_refactor, 1,0 ))
    , null )
as no_test_refactor_rate
, sum(if(general.bq_abstraction(lower(message)) > 0, 1, 0)) as textual_abstraction_commits
, 1.0*sum(if(general.bq_abstraction(lower(message)) > 0, 1, 0))/count(*) as textual_abstraction_commits_rate

, -1.0 as testing_involved_prob
, -1.0 as corrective_testing_involved_prob
, -1.0 as refactor_testing_involved_prob
, null as abs_content_ratio # We have data only in head, not per year


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


drop table if exists general.file_testing_pair_involvement_per_year;

create table general.file_testing_pair_involvement_per_year
as
select
repo_name
, file
, extract( year from commit_month) as year
, avg(if(test_involved, 1,0) ) as testing_involved_prob
, if( sum(if(is_corrective, 1,0)) > 0
    , 1.0*sum(if(test_involved and is_corrective, 1,0) )/sum(if(is_corrective, 1,0))
    , null) as corrective_testing_involved_prob
, if( sum(if(is_refactor, 1,0)) > 0
    , 1.0*sum(if(test_involved and is_refactor, 1,0) )/sum(if(is_refactor, 1,0))
    , null) as refactor_testing_involved_prob
from
general.testing_pairs_commits
group by
repo_name
, file
, year
;

update general.file_properties_per_year as fp
set testing_involved_prob = aux.testing_involved_prob
, corrective_testing_involved_prob = aux.corrective_testing_involved_prob
, refactor_testing_involved_prob = aux.refactor_testing_involved_prob
from
general.file_testing_pair_involvement_per_year as aux
where
fp.repo_name = aux.repo_name
and
fp.file = aux.file
and
fp.year = aux.year
;

drop table if exists general.file_testing_pair_involvement_per_year;