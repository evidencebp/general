drop table if exists general_ght.issue_status_period;


create table
general_ght.issue_status_period
as
select
prev.issue_id as issue_id
, max(i.repo_id) as repo_id
, max(prev.actor_id) as actor_id # The actor that started the period
, max(prev.action) as action # The action in the period
, max(prev.created_at) as from_date
, min(cur.created_at) as to_date
from
general_ght.issue_events as cur
join
general_ght.issue_events as prev
on
cur.issue_id = prev.issue_id
join
general_ght.issues as i
on
prev.issue_id = i.id
where
prev.created_at < cur.created_at
group by
prev.issue_id
;

drop table if exists general_ght.current_dates;


create table
general_ght.current_dates
as
SELECT cur_date
FROM UNNEST(GENERATE_DATE_ARRAY(
    DATE_SUB(CURRENT_DATE()
        , INTERVAL 10 year)
     , CURRENT_DATE()
     , INTERVAL 1 DAY)) AS cur_date;


drop table if exists general_ght.developer_wip_by_date;


create table
general_ght.developer_wip_by_date
as
select
p.repo_id
, p.actor_id as assignee
, d.cur_date
, count(distinct p.issue_id) as wip_tasks
from
general_ght.issue_status_period as p
join
general_ght.current_dates as d
on
date(p.from_date) <= d.cur_date
and
date(p.to_date) >= d.cur_date
where
action = 'assigned'
group by
p.repo_id
, p.actor_id
, d.cur_date
;

# WIP distribution
select
wip_tasks
, count(*) as cases
from
general_ght.developer_wip_by_date
group by
wip_tasks
order by
wip_tasks
;

drop table if exists general_ght.repo_wip;


create table
general_ght.repo_wip
as
select
repo_id
, avg(wip_tasks) as wip_tasks
, count(*) as cases
from
general_ght.developer_wip_by_date as w
group by
repo_id
order by
repo_id
;


drop table if exists general_ght.repo_wip_by_year;


create table
general_ght.repo_wip_by_year
as
select
repo_id
, extract(year from cur_date) as year
, avg(wip_tasks) as wip_tasks
, count(*) as cases
from
general_ght.developer_wip_by_date as w
group by
repo_id
, year
order by
repo_id
, year
;