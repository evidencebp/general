# commits_with_files.sql
drop table if exists general.commits_files_raw;

create table
general.commits_files_raw
partition by
commit_month
cluster by
repo_name, commit, file
as
select r.repo_name as repo_name
, difference.old_path as file
, commit
, cast(FORMAT_DATE('%Y-%m-01', DATE(TIMESTAMP_SECONDS(committer.date.seconds))) as date) as  commit_month
from
general.commits
cross join  UNNEST(repo_name) as commit_repo_name
cross join  UNNEST(difference) as difference
Join
general.repos as r
On commit_repo_name = r.Repo_name
;

drop table if exists general.commits_files;

create table
general.commits_files
partition by
commit_month
cluster by
repo_name, commit, file
as
select
cfr.repo_name as repo_name
, cfr.file as file
, cfr.commit
, ec.commit_month as  commit_month
, ec.author_name as author_name
, ec.author_email as author_email
, regexp_contains(lower(cfr.file), 'test') as is_test
, lower(reverse(substr(reverse(cfr.file), 0, strpos(reverse(cfr.file),'.')))) as extension
, lower(reverse(substr(reverse(cfr.file), 0, strpos(reverse(cfr.file),'.')))) in
('.bat', '.c', '.cc', '.coffee', '.cpp', '.cs', '.cxx', '.go',
       '.groovy', '.hs', '.java', '.js', '.lua', '.m',
       '.module', '.php', '.pl', '.pm', '.py', '.rb', '.s', '.scala',
       '.sh', '.swift', '.tpl', '.twig')
as code_extension
, ec.commit_timestamp  as commit_timestamp
, ec.is_corrective as is_corrective
, ec.is_adaptive as is_adaptive
, ec.is_perfective as is_perfective
, ec.is_English as is_English
, ec.is_refactor as is_refactor

from
general.commits_files_raw as cfr
join
general.enhanced_commits as ec
on
cfr.repo_name = ec.repo_name
and
cfr.commit = ec.commit
;

drop table if exists general.commits_files_raw;
