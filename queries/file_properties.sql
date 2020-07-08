drop table if exists general.file_properties_per_year;


create table
general.file_properties_per_year
partition by
commit_month
cluster by
year, repo_name, file
as
select
cf.repo_name as repo_name
, file
, extract( year from cf.commit_month) as year
, min(cf.commit_timestamp) as min_commit_in_year
, max(extension) as extension
, max(code_extension) as code_extension
, max(is_test) as is_test
, count(distinct cf.commit) as commits
, count(distinct case when cf.is_corrective  then cf.commit else null end) as corrective_commits
, 1.0*count(distinct case when cf.is_corrective  then cf.commit else null end)/count(distinct cf.commit) as corrective_rate
, 1.253*count(distinct case when cf.is_corrective  then cf.commit else null end)/count(distinct cf.commit) -0.053 as ccp
, avg(if(cf.is_corrective, null, non_test_files)) as avg_coupling_size
, avg(if(cf.is_corrective, null, code_non_test_files)) as avg_coupling_code_size
, avg(if(cf.is_corrective, null, if(non_test_files > 103 , 103 , non_test_files))) as avg_coupling_size_capped
, avg(if(cf.is_corrective, null, if(code_non_test_files> 103 , 103 ,code_non_test_files))) as avg_coupling_code_size_capped
, count(distinct cf.Author_email) as authors
, max(cf.Author_email) as Author_email # Meaningful only when authors=1
, min(cs.commit_month) as commit_month
from
general.commits_files as cf
join
general.commit_size as cs
on
cf.commit = cs.commit and cf.repo_name = cs.repo_name
and extract( year from cf.commit_month) =  extract( year from cs.commit_month)
group by
repo_name
, file
, year
;

drop table if exists general.file_properties;


create table
general.file_properties
partition by
commit_month
cluster by
repo_name, file
as
select
cf.repo_name as repo_name
, file
, min(cf.commit_timestamp) as min_commit
, max(extension) as extension
, max(code_extension) as code_extension
, max(is_test) as is_test
, count(distinct cf.commit) as commits
, count(distinct case when cf.is_corrective  then cf.commit else null end) as corrective_commits
, 1.0*count(distinct case when cf.is_corrective  then cf.commit else null end)/count(distinct cf.commit) as corrective_rate
, 1.253*count(distinct case when cf.is_corrective  then cf.commit else null end)/count(distinct cf.commit) -0.053 as ccp
, avg(if(cf.is_corrective, null, non_test_files)) as avg_coupling_size
, avg(if(cf.is_corrective, null, code_non_test_files)) as avg_coupling_code_size
, avg(if(cf.is_corrective, null, if(non_test_files > 103 , 103 , non_test_files))) as avg_coupling_size_capped
, avg(if(cf.is_corrective, null, if(code_non_test_files> 103 , 103 ,code_non_test_files))) as avg_coupling_code_size_capped
, count(distinct cf.Author_email) as authors
, max(cf.Author_email) as Author_email # Meaningful only when authors=1
, min(cs.commit_month) as commit_month
from
general.commits_files as cf
join
general.commit_size as cs
on
cf.commit = cs.commit and cf.repo_name = cs.repo_name
group by
repo_name
, file
;