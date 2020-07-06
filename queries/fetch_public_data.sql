# get general into repos table (upload if needed)
drop table if exists general.repos;

# Copy repositories relevant files
drop table if exists general.files;

create table
general.files
as
SELECT
f.*
 FROM
 `bigquery-public-data.github_repos.files` as f
 join
 general.repos as r on
 f.repo_name = r.repo_name
  ;

# Copy repositories relevant files' content
drop table if exists general.contents;

create table
general.contents
as
SELECT
cnt.*
# Note - all the properties bellow make compuation more efficient but do not exist in Google's scheme
, f.repo_name as repo_name
, f.path as path
, lower(reverse(substr(reverse(f.path), 0
, strpos(reverse(f.path),'.')))) as extension
 FROM
  `bigquery-public-data.github_repos.contents` as cnt
 join
 general.files as f
 on
 cnt.id = f.id
 ;


create table
general.commits
partition by
commit_month
cluster by
repo_name, commit
as
select
r.repo_name as repo_name
, commit
# Note - all the properties bellow make compuations more efficient but do not exist in Google's scheme
, max(author.name) as author_name
, max(author.email) as author_email
, max(DATE(TIMESTAMP_SECONDS(author.date.seconds))) as author_date
, max(TIMESTAMP_SECONDS(author.date.seconds)) as commit_timestamp
, max(subject) as subject
, max(message) as message
, max(cast(FORMAT_DATE('%Y-%m-01', DATE(TIMESTAMP_SECONDS(author.date.seconds))) as date)) as  commit_month
, count(distinct parent) as parents
from
`bigquery-public-data.github_repos.commits`
cross join  UNNEST(repo_name) as commit_repo_name
Join
general.repos as r
On commit_repo_name = r.Repo_name
cross join  UNNEST(parent) as parent
group by
r.repo_name
, commit
;