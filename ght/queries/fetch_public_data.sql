# get general into repos table (upload if needed)
# drop table if exists general.repos;


drop table if exists general_ght.projects;

create table
general_ght.projects
AS
select
 p.*
from
`ghtorrent-bq.ght.projects` as p
join
general.repos as r
on
r.repo_name = substr(p.url,length('https://api.github.com/repos/') +1)
;

drop table if exists general_ght.pull_requests;

create table
general_ght.pull_requests
AS
select
 pr.*
from
general_ght.projects as p
join
`ghtorrent-bq.ght.pull_requests` as pr
on
p.id = pr.base_repo_id
;

drop table if exists general_ght.commits;

create table
general_ght.commits
AS
select
c.*
from
general_ght.projects as p
join
`ghtorrent-bq.ght.commits` as c
on
p.id = c.project_id
;

drop table if exists general_ght.pull_request_commits;

create table
general_ght.pull_request_commits
AS
select
prc.*
from
`ghtorrent-bq.ght.pull_request_commits` as prc
join
general_ght.pull_requests as pr
on
#prc.pull_request_id = pr.pullreq_id
prc.pull_request_id = pr.id
join
general_ght.commits as c
on
prc.commit_id = c.id
;

drop table if exists general_ght.pull_request_history;

create table
general_ght.pull_request_history
AS
select
prh.*
from
`ghtorrent-bq.ght.pull_request_history` as prh
join
general_ght.pull_requests as pr
on
prh.pull_request_id = pr.id
;