drop table if exists general_ght.bug_after_pr;

create table
general_ght.bug_after_pr
as
select
epr.id as pull_request_id
, cf.commit
, min(cf.commit_timestamp) as commit_timestamp
, min(TIMESTAMP_DIFF(cf.commit_timestamp, epr.merged_at, DAY)) as days_to_first_bug
from
general_ght.enhanced_pull_requests as epr
join
general_ght.pull_request_commit_files as prcf
on
epr.id = prcf.pull_request_id
join
general.commits_files as cf
on
prcf.repo_name = cf.repo_name
and
prcf.file = cf.file
join
general.enhanced_commits as ec
on
cf.commit = ec.commit
where
cf.commit_timestamp > epr.merged_at
and
cf.is_corrective
group by
epr.id
, cf.commit
;




drop table if exists general_ght.pull_request_time_to_first_bug;

create table
general_ght.pull_request_time_to_first_bug
as
select
baf.pull_request_id as pull_request_id
, min(baf.commit_timestamp) as commit_timestamp
, min(baf.days_to_first_bug) as days_to_first_bug
, min(baf.commit) as bug_commit
from
general_ght.bug_after_pr as baf
join
(select
pull_request_id
, min(commit_timestamp) as commit_timestamp
from
general_ght.bug_after_pr
group by
pull_request_id) as minTime
on
baf.pull_request_id = minTime.pull_request_id
and
baf.commit_timestamp = minTime.commit_timestamp
group by
baf.pull_request_id
;

drop view if exists general_ght.pull_request_rejection;

create view
general_ght.pull_request_rejection
as
select
*
, merged_at is null as rejected
from
general_ght.enhanced_pull_requests as epr
where
opened_at is not null
;


drop table if exists general_ght.pull_request_context_180d;

create table
general_ght.pull_request_context_180d
as
select
epr.id
, prcf.file
, count(distinct if(cf.commit_timestamp >  epr.merged_at , cf.commit, null)) as commits_after
, if(count(distinct if(cf.commit_timestamp >  epr.merged_at, cf.commit, null)) > 0
    , 1.253*count(distinct case when cf.is_corrective
        and cf.commit_timestamp >  epr.merged_at then cf.commit else null end)
        /count(distinct if(cf.commit_timestamp >  epr.merged_at, cf.commit, null)) -0.053
   , null) as ccp_after
, if(count(distinct if(cf.commit_timestamp >  epr.merged_at, cf.commit, null)) > 0
    , 1.695*count(distinct case when cf.is_refactor
        and cf.commit_timestamp >  epr.merged_at then cf.commit else null end)
        /count(distinct if(cf.commit_timestamp >  epr.merged_at, cf.commit, null))-0.034
   , null) as refactor_mle_after
, avg(if(cf.commit_timestamp >  epr.merged_at and ec.same_date_as_prev, ec.duration, null)) as same_date_duration_after
, avg(if(cf.commit_timestamp >  epr.merged_at and not cf.is_corrective and parents = 1
    , if(non_test_files > 103 , 103 , non_test_files), null)) as avg_coupling_size_capped_after

, count(distinct if(cf.commit_timestamp <  epr.merged_at, cf.commit, null)) as commits_before
, if(count(distinct if(cf.commit_timestamp <  epr.merged_at, cf.commit, null)) > 0
    , 1.253*count(distinct case when cf.is_corrective
        and cf.commit_timestamp <  epr.merged_at then cf.commit else null end)
        /count(distinct if(cf.commit_timestamp <  epr.merged_at, cf.commit, null)) -0.053
   , null) as ccp_before
, if(count(distinct if(cf.commit_timestamp <  epr.merged_at, cf.commit, null)) > 0
    , 1.695*count(distinct case when cf.is_refactor
        and cf.commit_timestamp <  epr.merged_at then cf.commit else null end)
        /count(distinct if(cf.commit_timestamp <  epr.merged_at, cf.commit, null))-0.034
   , null) as refactor_mle_before
, avg(if(cf.commit_timestamp <  epr.merged_at and ec.same_date_as_prev, ec.duration, null)) as same_date_duration_before
, avg(if(cf.commit_timestamp <  epr.merged_at and not cf.is_corrective and parents = 1
    , if(non_test_files > 103 , 103 , non_test_files), null)) as avg_coupling_size_capped_before
from
general_ght.enhanced_pull_requests as epr
join
general_ght.pull_request_commit_files as prcf
on
epr.id = prcf.pull_request_id
join
general.commits_files as cf
on
prcf.repo_name = cf.repo_name
and
prcf.file = cf.file
and
abs(TIMESTAMP_DIFF(cf.commit_timestamp, epr.merged_at, DAY)) <= 6*30
join
general.enhanced_commits as ec
on
cf.repo_name = ec.repo_name
and
cf.commit = ec.commit
where
 epr.merged_at  is not null
group by
epr.id
, prcf.file
;



drop table if exists general_ght.pull_request_context_180d_improvement;

create table
general_ght.pull_request_context_180d_improvement
as
select
*
, ccp_after < ccp_before as ccp_improved
, ccp_after - ccp_before as ccp_diff

, same_date_duration_after < same_date_duration_before as same_date_duration_improved
, same_date_duration_after - same_date_duration_before as same_date_duration_diff

, avg_coupling_size_capped_after < avg_coupling_size_capped_before as avg_coupling_size_capped_improved
, avg_coupling_size_capped_after - avg_coupling_size_capped_before as avg_coupling_size_capped_diff

from
general_ght.pull_request_context_180d
;

select
count(*)
, avg(if(ccp_improved,1,0)) as ccp_improved_prob
, avg(if(same_date_duration_improved,1,0)) as same_date_duration_improved_prob
, avg(if(avg_coupling_size_capped_improved,1,0)) as avg_coupling_size_capped_improved_prob
from
general_ght.pull_request_context_180d_improvement;
