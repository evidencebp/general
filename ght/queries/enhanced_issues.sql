# enhanced_issues.sql
drop table if exists general_ght_large.issue_actions_agg;


create table general_ght_large.issue_actions_agg
as
select
issue_id
, min(if(action = 'assigned', created_at, null)) as assigned_at
, count(distinct if(action = 'assigned', event_id, null)) as assigned_num
, 0 as assigned_by

, min(if(action = 'closed', created_at, null)) as closed_at
, count(distinct if(action = 'closed', event_id, null)) as closed_num
, 0 as closed_by

, min(if(action = 'merged', created_at, null)) as merged_at
, count(distinct if(action = 'merged', event_id, null)) as merged_num
, 0 as merged_by

, min(if(action = 'reopened', created_at, null)) as reopened_at
, count(distinct if(action = 'reopened', event_id, null)) as reopened_num
, 0 as reopened_by

, min(if(action = 'milestoned', created_at, null)) as milestoned_at
, count(distinct if(action = 'milestoned', event_id, null)) as milestoned_num
, 0 as milestoned_by

, min(if(action = 'demilestoned', created_at, null)) as demilestoned_at
, count(distinct if(action = 'demilestoned', event_id, null)) as demilestoned_num
, 0 as demilestoned_by

from
general_ght_large.issue_events
group by
issue_id
;

# Assigned
drop table if exists general_ght_large.issue_assigned;

create table general_ght_large.issue_assigned
as
select
ie.issue_id
, min(ie.actor_id) as actor
from
general_ght_large.issue_events as ie
join
general_ght_large.issue_actions_agg as iaa
on
ie.issue_id = iaa.issue_id
and
ie.created_at = iaa.assigned_at
and
ie.action = 'assigned'
group by
ie.issue_id
having
count(distinct ie.actor_id) = 1 # Avoid cases of uncertainty
;


update general_ght_large.issue_actions_agg as iaa
set assigned_by = act.actor
from
general_ght_large.issue_assigned as act
where
iaa.issue_id = act.issue_id
;
drop table if exists general_ght_large.issue_assigned;


# Closed
drop table if exists general_ght_large.issue_closed;

create table general_ght_large.issue_closed
as
select
ie.issue_id
, min(ie.actor_id) as actor
from
general_ght_large.issue_events as ie
join
general_ght_large.issue_actions_agg as iaa
on
ie.issue_id = iaa.issue_id
and
ie.created_at = iaa.closed_at
and
ie.action = 'closed'
group by
ie.issue_id
having
count(distinct ie.actor_id) = 1 # Avoid cases of uncertainty
;


update general_ght_large.issue_actions_agg as iaa
set closed_by = act.actor
from
general_ght_large.issue_closed as act
where
iaa.issue_id = act.issue_id
;
drop table if exists general_ght_large.issue_closed;


# Merged
drop table if exists general_ght_large.issue_merged;

create table general_ght_large.issue_merged
as
select
ie.issue_id
, min(ie.actor_id) as actor
from
general_ght_large.issue_events as ie
join
general_ght_large.issue_actions_agg as iaa
on
ie.issue_id = iaa.issue_id
and
ie.created_at = iaa.merged_at
and
ie.action = 'merged'
group by
ie.issue_id
having
count(distinct ie.actor_id) = 1 # Avoid cases of uncertainty
;


update general_ght_large.issue_actions_agg as iaa
set merged_by = act.actor
from
general_ght_large.issue_merged as act
where
iaa.issue_id = act.issue_id
;
drop table if exists general_ght_large.issue_merged;


# reopened
drop table if exists general_ght_large.issue_reopened;

create table general_ght_large.issue_reopened
as
select
ie.issue_id
, min(ie.actor_id) as actor
from
general_ght_large.issue_events as ie
join
general_ght_large.issue_actions_agg as iaa
on
ie.issue_id = iaa.issue_id
and
ie.created_at = iaa.reopened_at
and
ie.action = 'reopened'
group by
ie.issue_id
having
count(distinct ie.actor_id) = 1 # Avoid cases of uncertainty
;


update general_ght_large.issue_actions_agg as iaa
set reopened_by = act.actor
from
general_ght_large.issue_reopened as act
where
iaa.issue_id = act.issue_id
;

drop table if exists general_ght_large.issue_reopened;


# milestoned
drop table if exists general_ght_large.issue_milestoned;

create table general_ght_large.issue_milestoned
as
select
ie.issue_id
, min(ie.actor_id) as actor
from
general_ght_large.issue_events as ie
join
general_ght_large.issue_actions_agg as iaa
on
ie.issue_id = iaa.issue_id
and
ie.created_at = iaa.milestoned_at
and
ie.action = 'milestoned'
group by
ie.issue_id
having
count(distinct ie.actor_id) = 1 # Avoid cases of uncertainty
;


update general_ght_large.issue_actions_agg as iaa
set milestoned_by = act.actor
from
general_ght_large.issue_milestoned as act
where
iaa.issue_id = act.issue_id
;
drop table if exists general_ght_large.issue_milestoned;


# demilestoned
drop table if exists general_ght_large.issue_demilestoned;

create table general_ght_large.issue_demilestoned
as
select
ie.issue_id
, min(ie.actor_id) as actor
from
general_ght_large.issue_events as ie
join
general_ght_large.issue_actions_agg as iaa
on
ie.issue_id = iaa.issue_id
and
ie.created_at = iaa.demilestoned_at
and
ie.action = 'demilestoned'
group by
ie.issue_id
having
count(distinct ie.actor_id) = 1 # Avoid cases of uncertainty
;


update general_ght_large.issue_actions_agg as iaa
set demilestoned_by = act.actor
from
general_ght_large.issue_demilestoned as act
where
iaa.issue_id = act.issue_id
;
drop table if exists general_ght_large.issue_demilestoned;


drop table if exists general_ght_large.enhanced_issues;

create table
general_ght_large.enhanced_issues
as
select
i.*
, assigned_at
, assigned_num
, assigned_by
, closed_at
, closed_num
, closed_by
, merged_at
, merged_num
, merged_by
, reopened_at
, reopened_num
, reopened_by
, milestoned_at
, milestoned_num
, milestoned_by
, demilestoned_at
, demilestoned_num
, demilestoned_by
, TIMESTAMP_DIFF(closed_at, assigned_at, MINUTE)
        as assigned_to_closed_minutes
, TIMESTAMP_DIFF(assigned_at, created_at, MINUTE)
        as created_to_assigned_minutes
, TIMESTAMP_DIFF(closed_at, created_at, MINUTE)
        as created_to_closed_minutes
from
general_ght_large.issues as i
left join
general_ght_large.issue_actions_agg as iaa
on
i.id = iaa.issue_id
;

drop table if exists general_ght_large.issue_actions_agg;


drop view if exists general_ght_large.issue_longer_than_week;

create view
general_ght_large.issue_longer_than_week
as
select
*
, TIMESTAMP_DIFF(closed_at, assigned_at, HOUR) >= 7*24 # About 93% are bellow
        as longer_than_week
from
general_ght_large.enhanced_issues
where
assigned_at is not null
and
TIMESTAMP_DIFF(closed_at, assigned_at, HOUR) >=0 # Avoid probably corrupted data
;


drop view if exists general_ght_large.issue_closed_unmerged;

create view
general_ght_large.issue_closed_unmerged
as
select
*
, merged_at is not null
        as merged
from
general_ght_large.enhanced_issues
where
closed_at is not null
;


drop view if exists general_ght_large.issue_reopen;

create view
general_ght_large.issue_reopen
as
select
*
, reopened_at is not null
        as reopened
from
general_ght_large.enhanced_issues
where
closed_at is not null
;

######### The code below is not working
######### It is an attempt to relate issue labels to issues
drop table if exists general_ght_large.enhanced_issue_labels;

create table
general_ght_large.enhanced_issue_labels
as
select
distinct
il.id as issue_id
, rl.repo_id as repo_id
, rl.name
from
general_ght_large.repo_labels as rl
join
general_ght_large.projects as p
on
rl.repo_id = p.id
join
general_ght_large.issue_labels as il
on
rl.repo_id = il.repo_id
#join
#general_ght_large.issues as i
#on
#il.id = i.id
;

# The query bellow returns duplicated
# The matching above mush be wrong
select
*
from
general_ght_large.enhanced_issue_labels
where
regexp_contains(name, 'External/Dependency')
;

select *
from general_ght_large.issues
where issue_id = cast(982 as string)
and
repo_id = 1924510;
#112303 issue
#1924510 project
#External/Dependency Issu

# Both conditions do not match
select count(*)
from general_ght_large.repo_labels as rl
join
general_ght_large.issues as i
on
rl.repo_id = i.repo_id
#and cast(rl.id as string) = i.issue_id
and rl.id = i.id
where name = 'External/Dependency Issu'
;

select
i.id is null as f
, count(*)
from general_ght_large.issue_labels as il
left join
general_ght_large.issues as i
on
cast(il.id as string) = i.issue_id
and
il.repo_id = i.repo_id
group by
f
;