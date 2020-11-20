
drop table if exists general_ght.pull_request_actions_agg;


create table general_ght.pull_request_actions_agg
as
select
pull_request_id
, min(created_at) as created_at
, min(if(action = 'opened', created_at, null)) as opened_at
, count(distinct if(action = 'opened', id, null)) as opened_num
, 0 as opened_by
, min(if(action = 'closed', created_at, null)) as closed_at
, count(distinct if(action = 'closed', id, null)) as closed_num
, 0 as closed_by
, min(if(action = 'merged', created_at, null)) as merged_at
, count(distinct if(action = 'merged', id, null)) as merged_num
, 0 as merged_by
from
general_ght.pull_request_history
group by
pull_request_id
;


drop table if exists general_ght.pull_request_opener;

create table general_ght.pull_request_opener
as
select
prh.pull_request_id
, min(prh.actor_id) as actor
from
general_ght.pull_request_history as prh
join
general_ght.pull_request_actions_agg as praa
on
praa.pull_request_id = prh.pull_request_id
and
prh.created_at = praa.opened_at
and
prh.action = 'opened'
group by
prh.pull_request_id
having
count(distinct prh.actor_id) = 1 # Avoid cases of uncertainty
;

update general_ght.pull_request_actions_agg as praa
set opened_by = pra.actor
from
general_ght.pull_request_opener as pra
where
praa.pull_request_id = pra.pull_request_id
;
drop table if exists general_ght.pull_request_opener;

drop table if exists general_ght.pull_request_closer;

create table general_ght.pull_request_closer
as
select
prh.pull_request_id
, min(prh.actor_id) as actor
from
general_ght.pull_request_history as prh
join
general_ght.pull_request_actions_agg as praa
on
praa.pull_request_id = prh.pull_request_id
and
prh.created_at = praa.opened_at
and
prh.action = 'close'
group by
prh.pull_request_id
having
count(distinct prh.actor_id) = 1 # Avoid cases of uncertainty
;

update general_ght.pull_request_actions_agg as praa
set closed_by = pra.actor
from
general_ght.pull_request_closer as pra
where
praa.pull_request_id = pra.pull_request_id
;
drop table if exists general_ght.pull_request_closer;

drop table if exists general_ght.pull_request_merger;

create table general_ght.pull_request_merger
as
select
prh.pull_request_id
, min(prh.actor_id) as actor
from
general_ght.pull_request_history as prh
join
general_ght.pull_request_actions_agg as praa
on
praa.pull_request_id = prh.pull_request_id
and
prh.created_at = praa.opened_at
and
prh.action = 'merged'
group by
prh.pull_request_id
having
count(distinct prh.actor_id) = 1 # Avoid cases of uncertainty
;

update general_ght.pull_request_actions_agg as praa
set merged_by = pra.actor
from
general_ght.pull_request_merger as pra
where
praa.pull_request_id = pra.pull_request_id
;
drop table if exists general_ght.pull_request_merger;


drop table if exists general_ght.pull_request_comments_agg;

create table
general_ght.pull_request_comments_agg
as
select
pull_request_id
, count(distinct user_id) as commenters_num
, STRING_AGG(cast(user_id as string)) as commenters
, count(distinct comment_id) as comments_num
from
general_ght.pull_request_comments
group by
pull_request_id
;

drop table if exists general_ght.pull_request_commits_agg;

create table
general_ght.pull_request_commits_agg
as
select
prc.pull_request_id as pull_request_id
, count(distinct sha) as commits
, count(distinct author_id ) as authors_num
, STRING_AGG(cast(author_id as string)) as authors
, min(author_id) as min_author # Make sense only if there is one author
, min(c.created_at) as first_commit_timestamp
, max(c.created_at) as last_commit_timestamp
from
general_ght.pull_request_commits as prc
join
general_ght.commits as c
on
prc.commit_id = c.id
group by
prc.pull_request_id
;

drop table if exists general_ght.enhanced_pull_requests;

create table
general_ght.enhanced_pull_requests
as
select
pr.*
, praa.created_at
, praa.opened_at
, praa.opened_num
, praa.opened_by
, praa.closed_at
, praa.closed_num
, praa.closed_by
, praa.merged_at
, praa.merged_num
, praa.merged_by

, if(praa.opened_at is null, null, if(praa.merged_at is null, 1, 0)) as is_rejected

, prca.commenters_num
, prca.commenters
, prca.comments_num


, pr_commits.commits
, pr_commits.authors_num
, pr_commits.authors
, pr_commits.min_author as author # Make sense only if there is one author
, pr_commits.first_commit_timestamp
, pr_commits.last_commit_timestamp

, TIMESTAMP_DIFF(pr_commits.last_commit_timestamp, pr_commits.first_commit_timestamp, MINUTE)
        as first_to_last_commit_minutes
, TIMESTAMP_DIFF(praa.merged_at, praa.opened_at, MINUTE)
        as open_to_merge_minutes
, TIMESTAMP_DIFF(praa.merged_at, pr_commits.first_commit_timestamp, MINUTE)
        as first_commit_to_merge_minutes
, -1 as days_to_first_bug
from
general_ght.pull_requests as pr
left join
general_ght.pull_request_actions_agg as praa
on
pr.id = praa.pull_request_id
left join
general_ght.pull_request_comments_agg as prca
on
pr.id = prca.pull_request_id
left join
general_ght.pull_request_commits_agg as pr_commits
on
pr.id = pr_commits.pull_request_id
;

drop table if exists general_ght.pull_request_actions_agg;
drop table if exists general_ght.pull_request_comments_agg;
drop table if exists general_ght.pull_request_commits_agg;


drop table if exists general_ght.pull_request_commit_files;

create table
general_ght.pull_request_commit_files
as
select
prc.*
, cf.*
from
general_ght.pull_request_commits as prc
join
general_ght.commits as gc
on
prc.commit_id = gc.id
join
general.commits_files as cf
on
gc.sha = cf.commit
;
