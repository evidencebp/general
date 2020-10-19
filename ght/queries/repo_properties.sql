drop table if exists general_ght.repo_issues_profile;

create table
general_ght.repo_issues_profile
as
select
p.name as repo_name
, count(distinct issue_id) as issues
, 1.0*count(distinct if(ie.assigned_at is null, null, issue_id))/count(distinct issue_id) as assigned_issues_ratio
from
general_ght.enhanced_issues as ie
join
general_ght.projects as p
on
ie.repo_id = p.id
group by
p.name
;
