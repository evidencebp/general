# Churn_and_onboarding.sql
drop table if exists general_large.repo_onboarding;


create table
general_large.repo_onboarding
as
select
repo_name
, 1.0*sum(if(commits >= 12,1,0))/count(*) as onboarding_prob
, 1.0*sum(if(TIMESTAMP_DIFF(max_commit_timestamp, min_commit_timestamp, day) > 365,1,0))
        /count(*) as retention_prob # In here is the probability to be active more than a year
from
general_large.developer_per_repo_profile
group by
repo_name
;


update general_large.repo_properties as rp
set
onboarding_prob = aux.onboarding_prob
, retention_prob = aux.retention_prob
from
general_large.repo_onboarding as aux
where
rp.repo_name = aux.repo_name
;

update general_large.repo_properties as rp
set
onboarding_prob = null
where
onboarding_prob = -1.0
;

update general_large.repo_properties as rp
set
retention_prob = null
where
retention_prob = -1.0
;

drop table if exists general_large.repo_onboarding;


### Per year
drop table if exists general_large.repo_onboarding_per_year;


create table
general_large.repo_onboarding_per_year
as
select
repo_name
, extract(year from min_commit_timestamp) as year
, if(count(*) > 0
,1.0*sum( if(commits >= 12,1,0))/
count(*)
, null) as onboarding_prob

, if(count(*) > 0
, 1.0*sum(if(TIMESTAMP_DIFF(max_commit_timestamp, min_commit_timestamp, day) > 365,1,0))/
count(*)
 , null) as retention_prob # In here is the probability to be active more than a year
from
general_large.developer_per_repo_profile as dp
group by
repo_name
, year
;


update general_large.repo_properties_per_year as rpy
set
onboarding_prob = aux.onboarding_prob
, retention_prob = aux.retention_prob
from
general_large.repo_onboarding_per_year as aux
where
rpy.repo_name = aux.repo_name
and
rpy.year = aux.year
;

update general_large.repo_properties_per_year as rp
set
onboarding_prob = null
where
onboarding_prob = -1.0
;

update general_large.repo_properties_per_year as rp
set
retention_prob = null
where
retention_prob = -1.0
;

drop table if exists general_large.repo_onboarding_per_year;

