drop table if exists general_ght.pull_requests_merge;

create table
general_ght.pull_requests_merge
as
select
pr.id as pr_id
, max(action = 'merged') as merged
from
general_ght.pull_requests as pr
left join
general_ght.pull_request_history as prh
on
pr.id = prh.pull_request_id
group by
pr_id
;
