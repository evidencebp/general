drop table if exists general.repo_onboarding;


create table
general.repo_onboarding
as
select
repo_name
, 1.0*sum(if(commits >= 12,1,0))/count(*) as onboarding_prob
, 1.0*sum(if(TIMESTAMP_DIFF(max_commit_timestamp, min_commit_timestamp, day) > 365,1,0))
        /count(*) as retention_prob # In here is the probability to be active more than a year
from
developer_per_repo_profile
group by
repo_name
;


update general.repo_properties as rp
set
onboarding_prob = aux.onboarding_prob
, retention_prob = aux.retention_prob
from
general.repo_onboarding as aux
where
rp.repo_name = aux.repo_name
;

drop table if exists general.repo_onboarding;


### Per year
drop table if exists general.repo_onboarding_per_year;


create table
general.repo_onboarding_per_year
as
select
repo_name
, year
, 1.0*sum(if(commits >= 12,1,0))/count(*) as onboarding_prob
, 1.0*sum(if(TIMESTAMP_DIFF(max_commit_timestamp, min_commit_timestamp, day) > 365,1,0))
        /count(*) as retention_prob # In here is the probability to be active more than a year
from
developer_per_repo_profile_per_year
group by
repo_name
, year
;


update general.repo_properties_per_year as rpy
set
onboarding_prob = aux.onboarding_prob
, retention_prob = aux.retention_prob
from
general.repo_onboarding_per_year as aux
where
rpy.repo_name = aux.repo_name
and
rpy.year = aux.year
;

drop table if exists general.repo_onboarding;
