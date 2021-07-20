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

drop function general.bq_effective_refactor_days;

CREATE OR REPLACE FUNCTION
general.bq_effective_refactor_days()
 RETURNS int64
AS (
3*30 # 3 months
 )
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


drop table if exists general.refactoring_stats_context;

create table
general.refactoring_stats_context
as
select
r.repo_name as repo_name
, r.commit as commit
, r.file
, max(cf.code_extension) as code_extension
, max(cf.is_test) as is_test
, max(ec.code_non_test_files) as code_non_test_files # Refactor's size
, count(distinct if(cf.commit_timestamp >  r.commit_timestamp, cf.commit, null)) as commits_after
, if(count(distinct if(cf.commit_timestamp >  r.commit_timestamp, cf.commit, null)) > 0
    , general.bq_ccp_mle(count(distinct case when cf.is_corrective
        and cf.commit_timestamp >  r.commit_timestamp then cf.commit else null end)
        /count(distinct if(cf.commit_timestamp >  r.commit_timestamp, cf.commit, null)))
   , null) as ccp_after
, if(count(distinct if(cf.commit_timestamp >  r.commit_timestamp, cf.commit, null)) > 0
    , general.bq_refactor_mle(count(distinct case when cf.is_refactor
        and cf.commit_timestamp >  r.commit_timestamp then cf.commit else null end)
        /count(distinct if(cf.commit_timestamp >  r.commit_timestamp, cf.commit, null)))
   , null) as refactor_mle_after
, avg(if(cf.commit_timestamp >  r.commit_timestamp and ec.same_date_as_prev, ec.duration, null)) as same_date_duration_after
, avg(if(cf.commit_timestamp >  r.commit_timestamp and not cf.is_corrective and parents = 1
    , if(code_non_test_files > 10 , 10 , non_test_files), null)) as coupling_code_size_cut_after

, if (sum(if(cf.commit_timestamp >  r.commit_timestamp and ec.is_corrective, 1,0 )) > 0
, 1.0*sum(if(cf.commit_timestamp >  r.commit_timestamp and  code_non_test_files = 1 and ec.is_corrective, 1,0 ))
    /sum(if(cf.commit_timestamp >  r.commit_timestamp and ec.is_corrective, 1,0 ))
, null)
as one_file_fix_rate_after
, if (sum(if(cf.commit_timestamp >  r.commit_timestamp and ec.is_refactor, 1,0 )) > 0
, 1.0*sum(if(cf.commit_timestamp >  r.commit_timestamp and  code_non_test_files = 1 and ec.is_refactor, 1,0 ))
/sum(if(cf.commit_timestamp >  r.commit_timestamp and ec.is_refactor, 1,0 ))
, null)
as one_file_refactor_rate_after


, count(distinct if(cf.commit_timestamp <  r.commit_timestamp, cf.commit, null)) as commits_before
, if(count(distinct if(cf.commit_timestamp <  r.commit_timestamp, cf.commit, null)) > 0
    , general.bq_ccp_mle(count(distinct case when cf.is_corrective
        and cf.commit_timestamp <  r.commit_timestamp then cf.commit else null end)
        /count(distinct if(cf.commit_timestamp <  r.commit_timestamp, cf.commit, null)))
   , null) as ccp_before
, if(count(distinct if(cf.commit_timestamp <  r.commit_timestamp, cf.commit, null)) > 0
    , general.bq_refactor_mle(count(distinct case when cf.is_refactor
        and cf.commit_timestamp <  r.commit_timestamp then cf.commit else null end)
        /count(distinct if(cf.commit_timestamp <  r.commit_timestamp, cf.commit, null)))
   , null) as refactor_mle_before
, avg(if(cf.commit_timestamp <  r.commit_timestamp and ec.same_date_as_prev, ec.duration, null)) as same_date_duration_before
, avg(if(cf.commit_timestamp <  r.commit_timestamp and not cf.is_corrective and parents = 1
    , if(non_test_files > 10 , 10 , non_test_files), null)) as coupling_code_size_cut_before

 , if (sum(if(cf.commit_timestamp <  r.commit_timestamp and ec.is_corrective, 1,0 )) > 0
, 1.0*sum(if(cf.commit_timestamp <  r.commit_timestamp and code_non_test_files = 1 and ec.is_corrective, 1,0 ))
    /sum(if(cf.commit_timestamp <  r.commit_timestamp and ec.is_corrective, 1,0 ))
, null)
as one_file_fix_rate_before
, if (sum(if(cf.commit_timestamp <  r.commit_timestamp and ec.is_refactor, 1,0 )) > 0
, 1.0*sum(if(cf.commit_timestamp <  r.commit_timestamp and  code_non_test_files = 1 and ec.is_refactor, 1,0 ))
    /sum(if(cf.commit_timestamp <  r.commit_timestamp and ec.is_refactor, 1,0 ))
, null)
as one_file_refactor_rate_before

from
general.refactoring_commits_files as r
join
general.commits_files as cf
on
r.repo_name = cf.repo_name
and
r.file = cf.file
join
general.enhanced_commits as ec
on
cf.repo_name = ec.repo_name
and
cf.commit = ec.commit
where
abs(TIMESTAMP_DIFF(cf.commit_timestamp, r.commit_timestamp, DAY)) <= general.bq_effective_refactor_days()
group by
r.repo_name
, r.commit
, r.file
;


drop table if exists general.refactoring_stats;

create table general.refactoring_stats
as
select
r.repo_name as repo_name
, r.commit as commit
, max(code_non_test_files) as code_non_test_files

, min(commits_after) as commits_after
, min(commits_before) as commits_before
, min(commits_before + commits_after) as commits_in_context

, max(refactor_mle_after) as refactor_mle_after # To filter other refactors in the same context
, max(refactor_mle_before) as refactor_mle_before

, count(if(code_extension
            and not is_test
            and ccp_before < ccp_after, file, null)) as ccp_worse
, count(if(code_extension
            and not is_test
            and same_date_duration_before < same_date_duration_after, file, null)) as same_date_duration_worse
, count(if(code_extension
            and not is_test
            and coupling_code_size_cut_before < coupling_code_size_cut_after, file, null)) as coupling_code_size_cut_worse

# Note that the good value of one file metrics are high, unlike ccp, duration and coupling
, count(if(code_extension
            and not is_test
            and one_file_fix_rate_before > one_file_fix_rate_after, file, null)) as one_file_fix_rate_worse
, count(if(code_extension
            and not is_test
            and one_file_refactor_rate_before > one_file_refactor_rate_after, file, null)) as one_file_refactor_rate_worse

, sum(if(code_extension
            and not is_test
            , ccp_before - ccp_after,  null)) as ccp_improvement
, sum(if(code_extension
            and not is_test
            , same_date_duration_before - same_date_duration_after,  null)) as same_date_duration_improvement
, sum(if(code_extension
            and not is_test
            , coupling_code_size_cut_before - coupling_code_size_cut_after,  null)) as coupling_code_size_cut_improvement
, sum(if(code_extension
            and not is_test
            , one_file_fix_rate_after - one_file_fix_rate_before ,  null)) as one_file_fix_rate_improvement
, sum(if(code_extension
            and not is_test
            , one_file_refactor_rate_after - one_file_refactor_rate_before ,  null)) as one_file_refactor_rate_improvement

from
general.refactoring_stats_context as r
group by
repo_name
, commit
;

select
count(distinct if(code_non_test_files <= 5
                    and refactor_mle_after <= 0
                    and refactor_mle_before <= 0
                    and commits_after >= 10
                    and commits_before >= 10
                      , commit, null)) as clean_refactor
, count(distinct if(code_non_test_files <= 5
                    and refactor_mle_after <= 0
                    and refactor_mle_before <= 0
                    and commits_after >= 10
                    and commits_before >= 10

                    and ccp_worse = 0
                    and same_date_duration_worse = 0
                    and coupling_code_size_cut_worse = 0
                    and one_file_fix_rate_worse = 0
                    and one_file_refactor_rate_worse = 0
                      , commit, null)) as clean_no_harm_refactor
, count(distinct if(code_non_test_files <= 5
                    and refactor_mle_after <= 0
                    and refactor_mle_before <= 0
                    and commits_after >= 10
                    and commits_before >= 10

                    and ccp_worse = 0
                    and same_date_duration_worse = 0
                    and coupling_code_size_cut_worse = 0
                    and one_file_fix_rate_worse = 0
                    and one_file_refactor_rate_worse = 0

                    and (ccp_improvement > 0
                            or same_date_duration_improvement > 0
                            or coupling_code_size_cut_improvement > 0
                            or one_file_fix_rate_improvement > 0
                            or one_file_refactor_rate_improvement > 0)
                      , commit, null)) as clean_good_refactor
, count(distinct if(code_non_test_files <= 5
                    and refactor_mle_after <= 0
                    and refactor_mle_before <= 0
                    and commits_after >= 10
                    and commits_before >= 10

                    and ccp_worse = 0
                    and same_date_duration_worse = 0
                    and coupling_code_size_cut_worse = 0
                    and one_file_fix_rate_worse = 0
                    and one_file_refactor_rate_worse = 0

                    and (ccp_improvement > 0.1
                            or same_date_duration_improvement > 10
                            or coupling_code_size_cut_improvement > 1
                            or one_file_fix_rate_improvement > 0.1
                            or one_file_refactor_rate_improvement > 0.1)
                      , commit, null)) as clean_really_good_refactor
, count(distinct if(code_non_test_files <= 5
                    and refactor_mle_after <= 0
                    and refactor_mle_before <= 0
                    and commits_after >= 10
                    and commits_before >= 10

                    and ccp_worse = 0
                    and same_date_duration_worse = 0
                    and coupling_code_size_cut_worse = 0
                    and one_file_fix_rate_worse = 0
                    and one_file_refactor_rate_worse = 0

                    and (ccp_improvement > 0.1
                            and same_date_duration_improvement > 10
                            and coupling_code_size_cut_improvement > 1
                            and one_file_fix_rate_improvement > 0.1
                            and one_file_refactor_rate_improvement > 0.1)
                      , commit, null)) as clean_unicorn_refactor


from
general.refactoring_stats

 select
 same_date_duration_before - 10 > same_date_duration_after as duration_improved
 , ccp_before - 0.1 > ccp_after as ccp_improved
 , count(*) as cases
 from
 general.refactoring_stats_context
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

drop table if exists general.refactoring_commits_files;
