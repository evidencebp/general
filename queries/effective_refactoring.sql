# first - second
# yyyy-mm-dd
WITH YourTable AS(
SELECT TIMESTAMP '2001-1-1' AS first, TIMESTAMP '2002-1-1' as second
UNION ALL SELECT TIMESTAMP '2002-1-1' AS first, TIMESTAMP '2001-1-1'
UNION ALL SELECT TIMESTAMP '2001-1-1', TIMESTAMP '2001-2-1'
UNION ALL SELECT TIMESTAMP  '2001-2-1', TIMESTAMP '2001-1-1'
UNION ALL SELECT TIMESTAMP '2001-1-1', TIMESTAMP '2001-1-2'
UNION ALL SELECT TIMESTAMP '2001-1-2' , TIMESTAMP  '2001-1-1'

)
SELECT  first, second, TIMESTAMP_DIFF(first, second, DAY)
FROM YourTable
;

drop table if exists general.refactoring_commits_files;

create table
general.refactoring_commits_files
partition by
commit_month
cluster by
repo_name, commit, file
as
select *
from
general.commits_files
where
is_refactor
;


drop table if exists general.refactoring_stats_context_180d;

create table
general.refactoring_stats_context_180d
as
select
r.repo_name as repo_name
, r.commit as commit
, r.file
, count(distinct if(cf.commit_timestamp >  r.commit_timestamp, cf.commit, null)) as commits_after
, if(count(distinct if(cf.commit_timestamp >  r.commit_timestamp, cf.commit, null)) > 0
    , 1.253*count(distinct case when cf.is_corrective
        and cf.commit_timestamp >  r.commit_timestamp then cf.commit else null end)
        /count(distinct if(cf.commit_timestamp >  r.commit_timestamp, cf.commit, null)) -0.053
   , null) as ccp_after
, if(count(distinct if(cf.commit_timestamp >  r.commit_timestamp, cf.commit, null)) > 0
    , 1.695*count(distinct case when cf.is_refactor
        and cf.commit_timestamp >  r.commit_timestamp then cf.commit else null end)
        /count(distinct if(cf.commit_timestamp >  r.commit_timestamp, cf.commit, null))-0.034
   , null) as refactor_mle_after
, avg(if(cf.commit_timestamp >  r.commit_timestamp and ec.same_date_as_prev, ec.duration, null)) as same_date_duration_after
, avg(if(cf.commit_timestamp >  r.commit_timestamp and not cf.is_corrective and parents = 1
    , if(non_test_files > 103 , 103 , non_test_files), null)) as avg_coupling_size_capped_after

, count(distinct if(cf.commit_timestamp <  r.commit_timestamp, cf.commit, null)) as commits_before
, if(count(distinct if(cf.commit_timestamp <  r.commit_timestamp, cf.commit, null)) > 0
    , 1.253*count(distinct case when cf.is_corrective
        and cf.commit_timestamp <  r.commit_timestamp then cf.commit else null end)
        /count(distinct if(cf.commit_timestamp <  r.commit_timestamp, cf.commit, null)) -0.053
   , null) as ccp_before
, if(count(distinct if(cf.commit_timestamp <  r.commit_timestamp, cf.commit, null)) > 0
    , 1.695*count(distinct case when cf.is_refactor
        and cf.commit_timestamp <  r.commit_timestamp then cf.commit else null end)
        /count(distinct if(cf.commit_timestamp <  r.commit_timestamp, cf.commit, null))-0.034
   , null) as refactor_mle_before
, avg(if(cf.commit_timestamp <  r.commit_timestamp and ec.same_date_as_prev, ec.duration, null)) as same_date_duration_before
, avg(if(cf.commit_timestamp <  r.commit_timestamp and not cf.is_corrective and parents = 1
    , if(non_test_files > 103 , 103 , non_test_files), null)) as avg_coupling_size_capped_before
from
general.refactoring_commits_files as r
join
general.commits_files as cf
on
r.repo_name = cf.repo_name
and
r.file = cf.file
and
abs(TIMESTAMP_DIFF(cf.commit_timestamp, r.commit_timestamp, DAY)) <= 6*30
join
general.enhanced_commits as ec
on
cf.repo_name = ec.repo_name
and
cf.commit = ec.commit
group by
r.repo_name
, r.commit
, r.file
;


 select
 same_date_duration_before - 10 > same_date_duration_after as duration_improved
 , ccp_before - 0.1 > ccp_after as ccp_improved
 , count(*) as cases
 from
 general.refactoring_stats_context_180d
 where
 commits_before >= 10
 and
 commits_after >= 10
 group by
 duration_improved
 , ccp_improved
 order by
 duration_improved
 , ccp_improved
 ;


 select
 same_date_duration_before - 10 > same_date_duration_after as duration_improved
 , avg_coupling_size_capped_before - 1 > avg_coupling_size_capped_after as coupling_improved
 , count(*) as cases
 from
 general.refactoring_stats_context_180d
 where
 commits_before >= 10
 and
 commits_after >= 10
 group by
 duration_improved
 , coupling_improved
 order by
 duration_improved
 , coupling_improved
 ;
