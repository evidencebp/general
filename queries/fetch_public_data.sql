# fetch_public_data.sql

# get general into repos table (upload if needed)
# drop table if exists general.repos;


# Update Is_Company in repos
update general.repos as r
set
Is_Company = aux.Is_Company
from
general.companies as aux
where
r.user = aux.user
;

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
# Note - all the properties bellow make computation more efficient but do not exist in Google's scheme
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

drop table if exists general.commits;

create table
general.commits
as
select
c.*
from
`bigquery-public-data.github_repos.commits` as c
cross join  UNNEST(repo_name) as commit_repo_name
Join
general.repos as r
On commit_repo_name = r.Repo_name
;

drop table if exists general.flat_commits;

create table
general.flat_commits
partition by
commit_month
cluster by
repo_name, commit
as
select
r.repo_name as repo_name
, commit
, max(author.name) as author_name
, max(author.email) as author_email
, max(cast(FORMAT_DATE('%Y-%m-01', DATE(TIMESTAMP_SECONDS(author.date.seconds))) as date)) as  commit_month
, max(TIMESTAMP_SECONDS(author.date.seconds)) as commit_timestamp
, max(subject) as subject
, max(message) as message
, count(distinct parent) as parents
from
`bigquery-public-data.github_repos.commits` as c
cross join  UNNEST(repo_name) as commit_repo_name
Join
general.repos as r
On
commit_repo_name = r.Repo_name
cross join  UNNEST(parent) as parent
group by
r.repo_name
, commit
;

drop table if exists general.enhanced_commits;

create table
general.enhanced_commits
partition by
commit_month
cluster by
repo_name, commit
as
select
c.*
, False as is_corrective
, False as is_adaptive
, False as is_perfective
, False as is_English
, False as is_refactor
, False as is_cursing
, False as is_positive_sentiment
, False as is_negative_sentiment
, False as is_performance
, False as is_security
, False as is_typo
# The values of the columns below will be overridden later
, -1 as files
, -1 as non_test_files
, -1 as code_files
, -1 as code_non_test_files
, -1 as duration
, commit as prev_commit
, commit_timestamp as prev_timestamp
, False as same_date_as_prev
from
general.flat_commits as c
;

update
general.enhanced_commits
set
files = null
, non_test_files  = null
, code_files  = null
, code_non_test_files = null
, duration  = null
where
true
;

update
general.enhanced_commits
set
is_corrective = (general.bq_corrective(message) > 0)
where
true
;


update
general.enhanced_commits
set
is_adaptive = (general.bq_adaptive(message) > 0)
where
true
;

update
general.enhanced_commits
set
is_perfective = (general.bq_perfective(message) > 0)
where
true
;

update
general.enhanced_commits
set
is_English = (general.bq_English(message) > 0)
where
true
;


update
general.enhanced_commits
set
is_refactor = (general.bq_refactor(message) > 0)
where
true
;

update
general.enhanced_commits
set
is_cursing = (general.bq_swearing(message) > 0)
where
true
;

update
general.enhanced_commits
set
is_positive_sentiment = (general.bq_positive_sentiment(message) > 0)
where
true
;

update
general.enhanced_commits
set
is_negative_sentiment = general.bq_negative_sentiment(message) > 0
where
true
;

update
general.enhanced_commits
set
is_performance = general.bq_performance(message) > 0
where
true
;

update
general.enhanced_commits
set
is_security = general.bq_security(message) > 0
where
true
;

update
general.enhanced_commits
set
is_typo = general.bq_typo(message) > 0
where
true
;

drop table if exists general.flat_commits;
