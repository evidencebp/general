drop table if exists general.commit_size;

create table
general.commit_size
partition by
commit_month
cluster by
repo_name, commit
as
Select
repo_name
, commit
, max(Author_email) as Author_email
, max(commit_timestamp) as commit_timestamp
, max(is_corrective) as is_corrective
, count(distinct file) as files
, count(distinct case when not is_test then file else null end) as non_test_files
, count(distinct case when is_test then file else null end) as test_files
, count(distinct case when not code_extension then file else null end) as non_code_files
, count(distinct case when code_extension then file else null end) as code_files
, count(distinct case when code_extension and not is_test then file else null end) as code_non_test_files
, max(commit_month) as  commit_month
from
general.commits_files
group by
repo_name
, commit
;