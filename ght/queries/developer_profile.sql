# GHT developer profile


# Developer profile
drop table if exists general_ght_large.assignee_issues_profile;

create table
general_ght_large.assignee_issues_profile
as
select
assignee_id
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
general_ght_large.enhanced_issues as ie
join
general_ght_large.projects as p
on
ie.repo_id = p.id
group by
assignee_id
;

drop table if exists general_ght_large.opener_pull_requests_profile;

create table
general_ght_large.opener_pull_requests_profile
as
select
opened_by
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
, 1.0*sum(if(days_to_first_bug <=7, 1,0))/sum(1) as sloppy_pr_ratio
from
general_ght_large.projects as p
join
general_ght_large.enhanced_pull_requests as epr
on
p.id = epr.base_repo_id
group by
opened_by
;


drop table if exists general_ght_large.dev_profile;

create table
general_ght_large.dev_profile
as
select
dp.*
, dm.author_id
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
, prp.sloppy_pr_ratio
from
general_large.developer_profile as dp
left join
general_ght_large.developer_matching as dm
on
dp.author_email = dm.author_email
left join
general_ght_large.assignee_issues_profile as ip
on
dm.author_id = ip.assignee_id
left join
general_ght_large.opener_pull_requests_profile as prp
on
dm.author_id = prp.opened_by
;

drop table if exists general_ght_large.assignee_issues_profile;
drop table if exists general_ght_large.opener_pull_requests_profile;

####

# Developer repo
drop table if exists general_ght_large.assignee_repo_issues_profile;

create table
general_ght_large.assignee_repo_issues_profile
as
select
p.repo_name as repo_name
, assignee_id
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
general_ght_large.enhanced_issues as ie
join
general_ght_large.projects as p
on
ie.repo_id = p.id
group by
p.repo_name
, assignee_id
;

drop table if exists general_ght_large.opener_repo_pull_requests_profile;

create table
general_ght_large.opener_repo_pull_requests_profile
as
select
p.repo_name as repo_name
, opened_by
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
, 1.0*sum(if(days_to_first_bug <=7, 1,0))/sum(1) as sloppy_pr_ratio
from
general_ght_large.projects as p
join
general_ght_large.enhanced_pull_requests as epr
on
p.id = epr.base_repo_id
group by
p.repo_name
, opened_by
;


drop table if exists general_ght_large.dev_repo_profile;

create table
general_ght_large.dev_repo_profile
as
select
dp.*
, dm.author_id
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
, prp.sloppy_pr_ratio
from
general_large.developer_per_repo_profile as dp
left join
general_ght_large.projects as p
on
dp.repo_name = p.repo_name
left join
general_ght_large.developer_matching as dm
on
dp.author_email = dm.author_email
left join
general_ght_large.assignee_repo_issues_profile as ip
on
p.repo_name = ip.repo_name
and
dm.author_id = ip.assignee_id
left join
general_ght_large.opener_repo_pull_requests_profile as prp
on
p.repo_name = prp.repo_name
and
dm.author_id = prp.opened_by
;

drop table if exists general_ght_large.assignee_repo_issues_profile;
drop table if exists general_ght_large.opener_repo_pull_requests_profile;


# Developer repo per year


##### Repo properties per year

drop table if exists general_ght_large.assignee_issues_profile_per_year;

create table
general_ght_large.assignee_issues_profile_per_year
as
select
p.repo_name as repo_name
, assignee_id
, rpy.year as year
, count(distinct if(extract(year from ie.created_at) = rpy.year, issue_id, null)) as issues
, count(distinct if(extract(year from ie.assigned_at)= rpy.year , issue_id, null)) as assigned_issues
, if(count(distinct if(extract(year from ie.created_at) = rpy.year, issue_id, null)) > 0
, 1.0*count(distinct if(extract(year from ie.assigned_at)= rpy.year , issue_id, null))
    /count(distinct if(extract(year from ie.created_at) = rpy.year, issue_id, null))
, null ) as assigned_issues_ratio
, count(distinct if(extract(year from ie.closed_at) = rpy.year, issue_id, null)) as closed_issues
, if(count(distinct if(extract(year from ie.created_at) = rpy.year, issue_id, null)) > 0
, 1.0*count(distinct if(extract(year from ie.closed_at) = rpy.year, issue_id, null))
/count(distinct if(extract(year from ie.created_at) = rpy.year, issue_id, null))
, null ) as closed_issues_ratio
, count(distinct if(extract(year from ie.merged_at) = rpy.year, issue_id, null)) as merged_issues
, if(count(distinct if(extract(year from ie.created_at) = rpy.year, issue_id, null)) > 0
, 1.0*count(distinct if(extract(year from ie.merged_at) = rpy.year, issue_id, null))
/count(distinct if(extract(year from ie.created_at) = rpy.year, issue_id, null))
, null ) as merged_issues_ratio
, count(distinct if(extract(year from ie.reopened_at) = rpy.year, issue_id, null)) as reopened_issues
, if(count(distinct if(extract(year from ie.created_at) = rpy.year, issue_id, null)) > 0
, 1.0*count(distinct if(extract(year from ie.reopened_at) = rpy.year, issue_id, null))
/count(distinct if(extract(year from ie.created_at) = rpy.year, issue_id, null))
, null ) as reopened_issues_ratio
, count(distinct if(extract(year from ie.milestoned_at) = rpy.year, issue_id, null)) as milestoned_issues
, if(count(distinct if(extract(year from ie.created_at) = rpy.year, issue_id, null)) > 0
, 1.0*count(distinct if(extract(year from ie.milestoned_at) = rpy.year, issue_id, null))
/count(distinct if(extract(year from ie.created_at) = rpy.year, issue_id, null))
, null ) as milestoned_issues_ratio
, count(distinct if(extract(year from ie.demilestoned_at) = rpy.year, issue_id, null)) as demilestoned_issues
, if(count(distinct if(extract(year from ie.created_at) = rpy.year, issue_id, null)) > 0
, 1.0*count(distinct if(extract(year from ie.demilestoned_at) = rpy.year, issue_id, null))
/count(distinct if(extract(year from ie.created_at) = rpy.year, issue_id, null))
, null ) as demilestoned_issues_ratio
, avg( assigned_to_closed_minutes ) as assigned_to_closed_minutes
, avg( created_to_assigned_minutes ) as created_to_assigned_minutes
, avg( created_to_closed_minutes ) as created_to_closed_minutes

from
general_large.repo_properties_per_year as rpy
join
general_ght_large.projects as p
on
rpy.repo_name = p.repo_name
join
general_ght_large.enhanced_issues as ie
on
ie.repo_id = p.id
group by
p.repo_name
, assignee_id
, rpy.year
;



drop table if exists general_ght_large.opener_pull_requests_profile_per_year;

create table
general_ght_large.opener_pull_requests_profile_per_year
as
select
p.repo_name as repo_name
, opened_by
, rpy.year as year
, count(distinct if(extract(year from epr.created_at) = rpy.year, epr.id, null)) as pull_requests
, count(distinct if(extract(year from epr.opened_at) = rpy.year, epr.id, null)) as opened_prs
, if(count(distinct if(extract(year from epr.created_at) = rpy.year, epr.id, null)) > 0
, 1.0*count(distinct if(extract(year from epr.opened_at) = rpy.year, epr.id, null))/
    count(distinct if(extract(year from epr.created_at) = rpy.year, epr.id, null))
, null) as opened_pr_ratio
, count(distinct if(extract(year from epr.closed_at) = rpy.year, epr.id, null)) as closed_prs
, if(count(distinct if(extract(year from epr.created_at) = rpy.year, epr.id, null)) > 0
, 1.0*count(distinct if(extract(year from epr.closed_at) = rpy.year, epr.id, null))/
    count(distinct if(extract(year from epr.created_at) = rpy.year, epr.id, null))
, null) as closed_pr_ratio
, count(distinct if(extract(year from epr.merged_at) = rpy.year, epr.id, null)) as merged_prs
, if(count(distinct if(extract(year from epr.created_at) = rpy.year, epr.id, null)) > 0
, 1.0*count(distinct if(extract(year from epr.merged_at) = rpy.year, epr.id, null))/
    count(distinct if(extract(year from epr.created_at) = rpy.year, epr.id, null))
, null) as merged_pr_ratio
, avg(if(extract(year from epr.created_at) = rpy.year, first_to_last_commit_minutes, null)) as first_to_last_commit_minutes
, avg(if(extract(year from epr.created_at) = rpy.year,open_to_merge_minutes, null)) as open_to_merge_minutes
, avg(if(extract(year from epr.created_at) = rpy.year,first_commit_to_merge_minutes, null)) as first_commit_to_merge_minutes
, if(sum(if(extract(year from epr.created_at) = rpy.year,1, 0)) > 0
, 1.0*sum(if(extract(year from epr.created_at) = rpy.year
 and days_to_first_bug <=7, 1,0))/sum(if(extract(year from epr.created_at) = rpy.year,1, 0))
, null) as sloppy_pr_ratio
from
general_large.repo_properties_per_year as rpy
join
general_ght_large.projects as p
on
rpy.repo_name = p.repo_name
join
general_ght_large.enhanced_pull_requests as epr
on
p.id = epr.base_repo_id
group by
p.repo_name
, opened_by
, rpy.year
;


drop table if exists general_ght_large.dev_repo_properties_per_year;


create table
general_ght_large.dev_repo_properties_per_year
as
select
dp.*
, dm.author_id
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
, prp.sloppy_pr_ratio
from
general_large.developer_per_repo_profile_per_year as dp
left join
general_ght_large.developer_matching as dm
on
dp.author_email = dm.author_email
left join
general_ght_large.assignee_issues_profile_per_year as ip
on
dm.author_id = ip.assignee_id
and
dp.repo_name = ip.repo_name
and
dp.year = ip.year
left join
general_ght_large.opener_pull_requests_profile_per_year as prp
on
dm.author_id = prp.opened_by
and
dp.repo_name = prp.repo_name
and
dp.year = prp.year
;

drop table if exists general_ght_large.assignee_issues_profile_per_year;
drop table if exists general_ght_large.opener_pull_requests_profile_per_year;
