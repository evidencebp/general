drop table if exists general.developer_per_repo_profile;

create table
general.developer_per_repo_profile
as
select
repo_name
, author_email
, author_name
, substr(author_email, STRPOS(author_email ,'@') + 1) as  author_email_domain
, count(distinct commit) as commits
, min(commit) as min_commit
, max(commit) as max_commit
from
general.enhanced_commits
#where
#commit_timestamp >= TIMESTAMP_ADD(current_timestamp(), INTERVAL -365 DAY)
group by
repo_name
, author_email
, author_name
;

drop table if exists general.developer_profile;

create table
general.developer_profile
as
select
author_email
, author_name
, substr(author_email, STRPOS(author_email ,'@') + 1) as  author_email_domain
, count(distinct commit) as commits
, min(commit) as min_commit
, max(commit) as max_commit
from
general.enhanced_commits
where
commit_timestamp >= TIMESTAMP_ADD(current_timestamp(), INTERVAL -365 DAY)
group by
author_email
, author_name
;
