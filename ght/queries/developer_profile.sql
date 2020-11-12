drop table if exists general_ght.assignee_issues_profile;

create table
general_ght.assignee_issues_profile
as
select
p.repo_name as repo_name
, count(distinct issue_id) as issues
, count(distinct if(ie.assigned_at is null, null, issue_id)) as assigned_issues
, 1.0*count(distinct if(ie.assigned_at is null, null, issue_id))/count(distinct issue_id) as assigned_issues_ratio
, count(distinct if(ie.closed_at is null, null, issue_id)) as closed_issues
, 1.0*count(distinct if(ie.closed_at is null, null, issue_id))/count(distinct issue_id) as closed_issues_ratio
, count(distinct if(ie.merged_at is null, null, issue_id)) as merged_issues
, 1.0*count(distinct if(ie.merged_at is null, null, issue_id))/count(distinct issue_id) as merged_issues_ratio
, count(distinct if(ie.reopened_at is null, null, issue_id)) as reopened_issues
, 1.0*count(distinct if(ie.reopened_at is null, null, issue_id))/count(distinct issue_id) as reopened_issues_ratio
, count(distinct if(ie. milestoned_at is null, null, issue_id)) as milestoned_issues
, 1.0*count(distinct if(ie.milestoned_at is null, null, issue_id))/count(distinct issue_id) as milestoned_issues_ratio
, count(distinct if(ie.demilestoned_at is null, null, issue_id)) as demilestoned_issues
, 1.0*count(distinct if(ie.demilestoned_at is null, null, issue_id))/count(distinct issue_id) as demilestoned_issues_ratio
, avg( assigned_to_closed_minutes ) as assigned_to_closed_minutes
, avg( created_to_assigned_minutes ) as created_to_assigned_minutes
, avg( created_to_closed_minutes ) as created_to_closed_minutes
from
general_ght.enhanced_issues as ie
join
general_ght.projects as p
on
ie.repo_id = p.id
group by
p.repo_name
;
