# effective_pull_request.sql
drop table if exists general_ght_large.bug_after_pr;

create table
general_ght_large.bug_after_pr
as
select
epr.id as pull_request_id
, cf.commit
, min(cf.commit_timestamp) as commit_timestamp
, min(TIMESTAMP_DIFF(cf.commit_timestamp, epr.merged_at, DAY)) as days_to_first_bug
from
general_ght_large.enhanced_pull_requests as epr
join
general_ght_large.pull_request_commit_files as prcf
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




drop table if exists general_ght_large.pull_request_time_to_first_bug;

create table
general_ght_large.pull_request_time_to_first_bug
as
select
baf.pull_request_id as pull_request_id
, min(baf.commit_timestamp) as commit_timestamp
, min(baf.days_to_first_bug) as days_to_first_bug
, min(baf.commit) as bug_commit
from
general_ght_large.bug_after_pr as baf
join
(select
pull_request_id
, min(commit_timestamp) as commit_timestamp
from
general_ght_large.bug_after_pr
group by
pull_request_id) as minTime
on
baf.pull_request_id = minTime.pull_request_id
and
baf.commit_timestamp = minTime.commit_timestamp
group by
baf.pull_request_id
;

drop view if exists general_ght_large.pull_request_rejection;

create view
general_ght_large.pull_request_rejection
as
select
*
, merged_at is null as rejected
from
general_ght_large.enhanced_pull_requests as epr
where
opened_at is not null
;


drop table if exists general_ght_large.pull_request_context_180d;

create table
general_ght_large.pull_request_context_180d
as
select
epr.id
, prcf.file
, count(distinct if(ec.commit_timestamp >  epr.merged_at , cf.commit, null)) as commits_after
, if(count(distinct if(ec.commit_timestamp >  epr.merged_at, cf.commit, null)) > 0
    , 1.253*count(distinct case when cf.is_corrective
        and ec.commit_timestamp >  epr.merged_at then cf.commit else null end)
        /count(distinct if(ec.commit_timestamp >  epr.merged_at, cf.commit, null)) -0.053
   , null) as ccp_after
, if(count(distinct if(ec.commit_timestamp >  epr.merged_at, cf.commit, null)) > 0
    , 1.695*count(distinct case when cf.is_refactor
        and ec.commit_timestamp >  epr.merged_at then cf.commit else null end)
        /count(distinct if(ec.commit_timestamp >  epr.merged_at, cf.commit, null))-0.034
   , null) as refactor_mle_after
, avg(if(ec.commit_timestamp >  epr.merged_at and ec.same_date_as_prev, ec.duration, null)) as same_date_duration_after
, avg(if(ec.commit_timestamp >  epr.merged_at and not cf.is_corrective and parents = 1
    , if(non_test_files > 103 , 103 , non_test_files), null)) as avg_coupling_size_capped_after

, count(distinct if(ec.commit_timestamp <  epr.merged_at, cf.commit, null)) as commits_before
, if(count(distinct if(ec.commit_timestamp <  epr.merged_at, cf.commit, null)) > 0
    , 1.253*count(distinct case when cf.is_corrective
        and ec.commit_timestamp <  epr.merged_at then cf.commit else null end)
        /count(distinct if(ec.commit_timestamp <  epr.merged_at, cf.commit, null)) -0.053
   , null) as ccp_before
, if(count(distinct if(ec.commit_timestamp <  epr.merged_at, cf.commit, null)) > 0
    , 1.695*count(distinct case when cf.is_refactor
        and ec.commit_timestamp <  epr.merged_at then cf.commit else null end)
        /count(distinct if(ec.commit_timestamp <  epr.merged_at, cf.commit, null))-0.034
   , null) as refactor_mle_before
, avg(if(ec.commit_timestamp <  epr.merged_at and ec.same_date_as_prev, ec.duration, null)) as same_date_duration_before
, avg(if(ec.commit_timestamp <  epr.merged_at and not cf.is_corrective and parents = 1
    , if(non_test_files > 103 , 103 , non_test_files), null)) as avg_coupling_size_capped_before
from
general_ght_large.enhanced_pull_requests as epr
join
general_ght_large.pull_request_commit_files as prcf
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
cf.repo_name = ec.repo_name
and
cf.commit = ec.commit
and
abs(TIMESTAMP_DIFF(ec.commit_timestamp, epr.merged_at, DAY)) <= 6*30
where
 epr.merged_at  is not null
group by
epr.id
, prcf.file
;




drop table if exists general_ght_large.pull_request_file_context_180d_improvement;

create table
general_ght_large.pull_request_file_context_180d_improvement
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
general_ght_large.pull_request_context_180d
;


drop table if exists general_ght_large.effective_pull_request_180d;

create table
general_ght_large.effective_pull_request_180d
as
select
id
, count(distinct file) as files
, avg(commits_before) as commits_before
, avg(commits_after) as commits_after

, avg(ccp_before) as ccp_before
, avg(same_date_duration_before) as same_date_duration_before
, avg(avg_coupling_size_capped_before) as avg_coupling_size_capped_before

, sum(ccp_diff) as ccp_diff_sum
, sum(ccp_diff) > 0 as ccp_improved_sum
, avg(ccp_diff) as ccp_diff_avg
, avg(ccp_diff) > 0 as ccp_improved_avg

, sum(same_date_duration_diff) as same_date_duration_diff_sum
, sum(same_date_duration_diff) > 0 as same_date_duration_improved_sum
, avg(same_date_duration_diff) as same_date_duration_diff_avg
, avg(same_date_duration_diff) > 0 as same_date_duration_improved_avg

, sum(avg_coupling_size_capped_diff) as avg_coupling_size_capped_diff_sum
, sum(avg_coupling_size_capped_diff) > 0 as avg_coupling_size_capped_improved_sum
, avg(avg_coupling_size_capped_diff) as avg_coupling_size_capped_diff_avg
, avg(avg_coupling_size_capped_diff) > 0 as avg_coupling_size_capped_improved_avg

, count(if(ccp_improved and same_date_duration_improved and commits_before > 10 and commits_after > 10 , file, null)) as improved
, count(if(not ccp_improved or not same_date_duration_improved, file, null)) as degragated
from
general_ght_large.pull_request_file_context_180d_improvement
group by
id
;


update general_ght_large.enhanced_pull_requests as epr
set days_to_first_bug = fb.days_to_first_bug
from
general_ght_large.pull_request_time_to_first_bug as fb
where
epr.id = fb.pull_request_id
;

# Note - the default was set to -1 to make the column numeric
# Changed now to unknown
update general_ght_large.enhanced_pull_requests as epr
set days_to_first_bug = null
where
days_to_first_bug = -1
;
