# commit_size.sql
drop table if exists general.commit_size;

create table
general.commit_size
as
Select
repo_name
, commit
, max(Author_email) as Author_email
, max(commit_timestamp) as commit_timestamp
, max(is_corrective) as is_corrective
, count(distinct file) as files
, count(distinct case when not is_test then file else null end) as non_test_files
, count(distinct case when code_extension then file else null end) as code_files
, count(distinct case when code_extension and not is_test then file else null end) as code_non_test_files
, max(commit_month) as  commit_month
from
general.commits_files
group by
repo_name
, commit
;


UPDATE  general.enhanced_commits AS ec
SET
ec.files = cs.files
, ec.non_test_files = cs.non_test_files
, ec.code_files = cs.code_files
, ec.code_non_test_files = cs.code_non_test_files
FROM general.commit_size as cs
WHERE
ec.repo_name =  cs.repo_name
and
ec.commit =  cs.commit
;

drop table if exists general.commit_size;
