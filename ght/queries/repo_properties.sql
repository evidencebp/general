drop table if exists general_ght.repo_issues_profile;

create table
general_ght.repo_issues_profile
as
select
p.name as repo_name
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
p.name
;

drop table if exists general_ght.pull_requests_profile;

create table
general_ght.pull_requests_profile
as
select
p.name as repo_name
, count(distinct epr.id) as pull_requests
, count(distinct if(epr.opened_at is null, null, epr.id)) as opened_prs
, 1.0*count(distinct if(epr.opened_at is null, null, epr.id))/count(distinct epr.id) as opened_pr_ratio
, count(distinct if(epr.closed_at is null, null, epr.id)) as closed_prs
, 1.0*count(distinct if(epr.closed_at is null, null, epr.id))/count(distinct epr.id) as closed_pr_ratio
, count(distinct if(epr.merged_at is null, null, epr.id)) as merged_prs
, 1.0*count(distinct if(epr.merged_at is null, null, epr.id))/count(distinct epr.id) as merged_pr_ratio
, avg(first_to_last_commit_minutes) as first_to_last_commit_minutes
, avg(open_to_merge_minutes) as open_to_merge_minutes
, avg(first_commit_to_merge_minutes) as first_commit_to_merge_minutes
from
general_ght.projects as p
join
general_ght.enhanced_pull_requests as epr
on
p.id = epr.base_repo_id
group by
p.name
;

drop table if exists general_ght.repo_profile;

create table
general_ght.repo_profile
as
select
p.name as repo_name
, ip.issues
, ip.assigned_issues
, ip.assigned_issues_ratio
, ip.closed_issues
, ip.closed_issues_ratio
, ip.merged_issues
, ip.merged_issues_ratio
, ip.reopened_issues
, ip.reopened_issues_ratio
, ip.milestoned_issues
, ip.milestoned_issues_ratio
, ip.demilestoned_issues
, ip.demilestoned_issues_ratio
, ip.assigned_to_closed_minutes
, ip.created_to_assigned_minutes
, ip.created_to_closed_minutes
, prp.pull_requests
, prp.opened_prs
, prp.opened_pr_ratio
, prp.closed_prs
, prp.closed_pr_ratio
, prp.merged_prs
, prp.merged_pr_ratio
, prp.first_to_last_commit_minutes
, prp.open_to_merge_minutes
, prp.first_commit_to_merge_minutes
from
general_ght.projects as p
left join
general_ght.repo_issues_profile as ip
on
p.name = ip.repo_name
left join
general_ght.pull_requests_profile as prp
on
p.name = prp.repo_name
;

drop table if exists general_ght.repo_issues_profile;
drop table if exists general_ght.pull_requests_profile;


##### Repo properties per year

drop table if exists general_ght.repo_issues_profile_per_year;

create table
general_ght.repo_issues_profile_per_year
as
select
p.name as repo_name
, extract(year from ie.created_at) as year
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
p.name
, extract(year from ie.created_at)
;


drop table if exists general_ght.pull_requests_profile_per_year;

create table
general_ght.pull_requests_profile_per_year
as
select
p.name as repo_name
, extract(year from epr.opened_at) as year
, count(distinct epr.id) as pull_requests
, count(distinct if(epr.opened_at is null, null, epr.id)) as opened_prs
, 1.0*count(distinct if(epr.opened_at is null, null, epr.id))/count(distinct epr.id) as opened_pr_ratio
, count(distinct if(epr.closed_at is null, null, epr.id)) as closed_prs
, 1.0*count(distinct if(epr.closed_at is null, null, epr.id))/count(distinct epr.id) as closed_pr_ratio
, count(distinct if(epr.merged_at is null, null, epr.id)) as merged_prs
, 1.0*count(distinct if(epr.merged_at is null, null, epr.id))/count(distinct epr.id) as merged_pr_ratio
, avg(first_to_last_commit_minutes) as first_to_last_commit_minutes
, avg(open_to_merge_minutes) as open_to_merge_minutes
, avg(first_commit_to_merge_minutes) as first_commit_to_merge_minutes
from
general_ght.projects as p
join
general_ght.enhanced_pull_requests as epr
on
p.id = epr.base_repo_id
group by
p.name
, extract(year from epr.opened_at)
;

drop table if exists general_ght.repo_properties_per_year;


create table
general_ght.repo_properties_per_year
as
select
rpy.*
, ip.issues
, ip.assigned_issues
, ip.assigned_issues_ratio
, ip.closed_issues
, ip.closed_issues_ratio
, ip.merged_issues
, ip.merged_issues_ratio
, ip.reopened_issues
, ip.reopened_issues_ratio
, ip.milestoned_issues
, ip.milestoned_issues_ratio
, ip.demilestoned_issues
, ip.demilestoned_issues_ratio
, ip.assigned_to_closed_minutes
, ip.created_to_assigned_minutes
, ip.created_to_closed_minutes
, prp.pull_requests
, prp.opened_prs
, prp.opened_pr_ratio
, prp.closed_prs
, prp.closed_pr_ratio
, prp.merged_prs
, prp.merged_pr_ratio
, prp.first_to_last_commit_minutes
, prp.open_to_merge_minutes
, prp.first_commit_to_merge_minutes
from
general.repo_properties_per_year as rpy
left join
general_ght.repo_issues_profile_per_year as ip
on
rpy.repo_name = ip.repo_name
and
rpy.year = ip.year
left join
general_ght.pull_requests_profile_per_year as prp
on
rpy.repo_name = prp.repo_name
and
rpy.year = prp.year
;

drop table if exists general_ght.repo_issues_profile_per_year;
drop table if exists general_ght.pull_requests_profile_per_year;
