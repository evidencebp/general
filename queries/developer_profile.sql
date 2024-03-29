##### Creating developer_profile

drop table if exists general.developer_profile;

create table
general.developer_profile
as
select
author_email
, max(author_name) as author_name
, count( distinct author_name) as names # For safety, see if the email has some names
, substr(author_email, STRPOS(author_email ,'@') + 1) as  author_email_domain
, count(distinct repo_name) as repositories
, count(distinct if(substr(repo_name, 0, STRPOS(repo_name ,'/') -1) = author_name, repo_name, null)) as owned_repositories
, min(ec.commit_timestamp) as min_commit_timestamp
, max(ec.commit_timestamp) as max_commit_timestamp

, count(distinct commit) as commits
, min(commit) as min_commit
, max(commit) as max_commit

, 0 as files_edited
, 0 as files_created
, 0 as files_owned

, 0.0 as files_edited_ccp
, 0.0 as files_created_ccp
, 0.0 as files_owned_ccp

, 1.0*count(distinct if(is_corrective, commit, null))/count(distinct commit) as corrective_rate
, general.bq_ccp_mle(1.0*count(distinct if(is_corrective, commit, null))/count(distinct commit)) as ccp

, general.bq_refactor_mle(1.0*count(distinct case when is_refactor  then commit else null end)/count(distinct commit))
        as refactor_mle

, 1.0*count(distinct if(is_cursing, commit, null))/ count(distinct commit) as cursing_rate

, 1.0*count(distinct case when is_positive_sentiment then commit else null end)/count(distinct commit) as positive_sentiment_rate
, 1.0*count(distinct case when is_negative_sentiment then commit else null end)/count(distinct commit) as negative_sentiment_rate

, avg(if(not is_corrective, non_test_files, null)) as avg_coupling_size
, avg(if(not is_corrective, code_non_test_files, null)) as avg_coupling_code_size
, avg(if(not is_corrective, if(non_test_files > 103 , 103 , non_test_files), null)) as avg_coupling_size_capped
, avg(if(not is_corrective, if(code_non_test_files> 103 , 103 ,code_non_test_files), null)) as avg_coupling_code_size_capped
, avg(if(not is_corrective, if(non_test_files > 103 , null , non_test_files), null)) as avg_coupling_size_cut
, avg(if(not is_corrective, if(code_non_test_files> 10 , null ,code_non_test_files), null)) as avg_coupling_code_size_cut


, 0.0 as tests_presence

# Commit message linguistic characteristic (e.g., message length)
, 1.0*count(distinct if(REGEXP_CONTAINS(message,'\\n'), commit, null))/ count(distinct commit)
as multiline_message_ratio
, avg(length(message)) as message_length_avg

, if(count(distinct case when is_corrective  then commit else null end) > 0
,1.0*count(distinct if(is_corrective and REGEXP_CONTAINS(message,'\\n'), commit, null))
/ count(distinct case when is_corrective  then commit else null end)
, null)
as corrective_multiline_message_ratio
, avg(if(is_corrective,length(message), null)) as corrective_message_length_avg

, if(count(distinct case when not is_corrective  then commit else null end) > 0
,1.0*count(distinct if(not is_corrective and REGEXP_CONTAINS(message,'\\n'), commit, null))
/ count(distinct case when not is_corrective  then commit else null end)
, null)
as non_corrective_multiline_message_ratio
, avg(if(not is_corrective,length(message), null)) as non_corrective_message_length_avg


#	\items Commits/distinct commits variation \cite{8952390} \idan{Consider more ideas from there}
# Developer Reputation Estimator (DRE)
, 1.0*count(*)/count(distinct commit) as duplicated_commits_ratio


# Duration
, avg(case when same_date_as_prev then duration else null end) as same_date_duration_avg
, count(distinct case when same_date_as_prev then commit else null end) as same_date_commits

, TIMESTAMP_DIFF(max(ec.commit_timestamp), min(ec.commit_timestamp), day) as commit_period
, count(distinct date(ec.commit_timestamp)) as commit_days
, if(count(distinct date(ec.commit_timestamp)) > 0
 , 1.0*count(distinct commit)/count(distinct date(ec.commit_timestamp))
 , 1.0*count(distinct commit))as commits_per_day

, count(distinct extract(week from date(ec.commit_timestamp))) as commit_weeks
, count(distinct extract(month from date(ec.commit_timestamp))) as commit_months
, count(distinct extract(dayofweek from date(ec.commit_timestamp))) as commit_days_of_week
, count(distinct extract(hour from ec.commit_timestamp)) as commit_hours # Should see how diverse are the hours

# Density
, count(distinct ec.commit_timestamp)/(1+TIMESTAMP_DIFF(max(ec.commit_timestamp), min(ec.commit_timestamp), day)) as commit_day_density

# Week days prob
, 1.0*count(distinct case when extract(DAYOFWEEK from date(ec.commit_timestamp))= 1 then ec.commit else null end)
    /count(distinct ec.commit) as Sunday_prob
, 1.0*count(distinct case when extract(DAYOFWEEK from date(ec.commit_timestamp))= 2 then ec.commit else null end)
    /count(distinct ec.commit) as Monday_prob
, 1.0*count(distinct case when extract(DAYOFWEEK from date(ec.commit_timestamp))= 3 then ec.commit else null end)
    /count(distinct ec.commit) as Tuesday_prob
, 1.0*count(distinct case when extract(DAYOFWEEK from date(ec.commit_timestamp))= 4 then ec.commit else null end)
    /count(distinct ec.commit) as Wednesday_prob
, 1.0*count(distinct case when extract(DAYOFWEEK from date(ec.commit_timestamp))= 5 then ec.commit else null end)
    /count(distinct ec.commit) as Thursday_prob
, 1.0*count(distinct case when extract(DAYOFWEEK from date(ec.commit_timestamp))= 6 then ec.commit else null end)
    /count(distinct ec.commit) as Friday_prob
, 1.0*count(distinct case when extract(DAYOFWEEK from date(ec.commit_timestamp))= 7 then ec.commit else null end)
    /count(distinct ec.commit) as Saturday_prob
, 0.0 as days_entropy
, 0.0 as hour_entropy

, 0.0 as avg_file_size
, 0.0 as capped_avg_file_size
, 0.0 as avg_code_file_size
, 0.0 as capped_avg_code_file_size

, 0.0 as sum_file_size
, 0.0 as capped_sum_file_size
, 0.0 as sum_code_file_size
, 0.0 as capped_sum_code_file_size


, 0.0 as prev_touch_ago
, 0.0 as bug_prev_touch_ago


,  avg(if(same_date_as_prev, duration, null)) as same_day_duration_avg

# Abstraction
, if (sum(if(ec.is_corrective, 1,0 )) > 0
, 1.0*sum(if( code_non_test_files = 1 and ec.is_corrective, 1,0 ))/sum(if(ec.is_corrective, 1,0 ))
, null)
as one_file_fix_rate
, if (sum(if(ec.is_refactor, 1,0 )) > 0
, 1.0*sum(if( code_non_test_files = 1 and ec.is_refactor, 1,0 ))/sum(if(ec.is_refactor, 1,0 ))
, null)
as one_file_refactor_rate

, if(sum(if((code_non_test_files = 1 and code_files = 2 ) or code_files=1, 1,0 )) > 0
    , 1.0*sum(if(code_files=1, 1,0 ))/sum(if((code_non_test_files = 1 and code_files = 2 ) or code_files=1, 1,0 ))
    , null)
as test_usage_rate

, if(sum(if(ec.is_refactor and ((code_non_test_files = 1 and code_files = 2 ) or code_files=1), 1,0 )) > 0
    , 1.0*sum(if(ec.is_refactor and code_files=1, 1,0 ))
        /sum(if(ec.is_refactor and ((code_non_test_files = 1 and code_files = 2 ) or code_files=1), 1,0 ))
    , null)
as test_usage_in_refactor_rate

, if(sum(if(ec.is_refactor, 1,0 )) > 0
    , 1.0*sum(if( code_non_test_files = code_files and ec.is_refactor, 1,0 ))/sum(if(ec.is_refactor, 1,0 ))
    , null )
as no_test_refactor_rate
, sum(if(general.bq_abstraction(lower(message)) > 0, 1, 0)) as textual_abstraction_commits
, 1.0*sum(if(general.bq_abstraction(lower(message)) > 0, 1, 0))/count(*) as textual_abstraction_commits_rate

, avg(cast(ec.is_typo as int64)) as typo_rate

, sum(if(cast(ec.is_corrective as int64) + cast(ec.is_adaptive as int64) + cast(ec.is_refactor as int64) > 1,1,0))/count(distinct ec.commit) as tangling_rate
, sum(if(cast(ec.is_corrective as int64) + cast(ec.is_adaptive as int64) + cast(ec.is_refactor as int64) = 3,1,0))/count(distinct ec.commit) as bingo_rate


, -1.0 as testing_involved_prob
, -1.0 as corrective_testing_involved_prob
, -1.0 as refactor_testing_involved_prob
, null as abs_content_ratio

, -1.0 as minutes_to_revert
, -1.0 as reverted_ratio

, count(distinct if(is_performance, ec.commit, null))/count(distinct ec.commit) as performance_rate
, count(distinct if(is_security, ec.commit, null))/count(distinct ec.commit) as security_rate

# Duration in project
# Join time relative to project creation
# Percent of effective refactors
from
general.enhanced_commits as ec
#where
#commit_timestamp >= TIMESTAMP_ADD(current_timestamp(), INTERVAL -365 DAY)
group by
author_email
;

update general.developer_profile
set days_entropy = - (case when Sunday_prob > 0 then Sunday_prob*log(Sunday_prob,2) else 0 end
                        + case when Monday_prob > 0 then Monday_prob*log(Monday_prob,2) else 0 end
                        + case when Tuesday_prob > 0 then Tuesday_prob*log(Tuesday_prob,2) else 0 end
                        + case when Wednesday_prob > 0 then Wednesday_prob*log(Wednesday_prob,2) else 0 end
                        + case when Thursday_prob > 0 then Thursday_prob*log(Thursday_prob,2) else 0 end
                        + case when Friday_prob > 0 then Friday_prob*log(Friday_prob,2) else 0 end
                        + case when Saturday_prob > 0 then Saturday_prob*log(Saturday_prob,2) else 0 end
)
, prev_touch_ago = null
, bug_prev_touch_ago = null
where true
;

drop table if exists general.author_created_files;

create table
general.author_created_files
as
select
creator_email
, count(distinct file) as files
, general.bq_ccp_mle(1.0*sum(corrective_commits)/sum(commits)) as ccp
from
general.file_properties
group by
creator_email
;

update general.developer_profile as dp
set files_created = acf.files, files_created_ccp = acf.ccp
from
general.author_created_files as acf
where
dp.author_email = acf.creator_email
;

drop table if exists general.author_created_files;



drop table if exists general.author_owned_files;

create table
general.author_owned_files
as
select
Author_email
, count(distinct concat(repo_name, file)) as files
, general.bq_ccp_mle(1.0*sum(corrective_commits)/sum(commits)) as ccp
, sum(prev_touch_ago*commits)/sum(commits) as prev_touch_ago
, if(sum(corrective_commits) > 0
    , sum(bug_prev_touch_ago*corrective_commits)/sum(corrective_commits)
     , null) as bug_prev_touch_ago
from
general.file_properties
where
authors = 1
group by
Author_email
;

update general.developer_profile as dp
set
files_owned = aof.files
, files_owned_ccp = aof.ccp
, prev_touch_ago = aof.prev_touch_ago
, bug_prev_touch_ago = aof.bug_prev_touch_ago
from
general.author_owned_files as aof
where
dp.author_email = aof.Author_email
;

drop table if exists general.author_owned_files;



drop table if exists general.author_edited_files;

create table
general.author_edited_files
as
select
author_email
, count(distinct concat(repo_name, file)) as files
, general.bq_ccp_mle(1.0*count(distinct if(is_corrective, commit, null))/count(distinct commit)) as ccp
, sum(if(is_test, 1,0))/count(*)  as tests_presence
from
general.commits_files
group by
author_email
;


update general.developer_profile as dp
set files_edited = aef.files, files_edited_ccp = aef.ccp, tests_presence = aef.tests_presence
from
general.author_edited_files as aef
where
dp.author_email = aef.author_email
;
drop table if exists general.author_edited_files;

drop table if exists general.developer_per_repo_profile;


drop table if exists general.dev_length_properties;


create table
general.dev_length_properties
as
select
creator_email as author_email
, avg(size) as avg_file_size
, avg(if(size > 180000, 180000, size)) as capped_avg_file_size
, avg(if( extension in ('.bat', '.c', '.cc', '.coffee', '.cpp', '.cs', '.cxx', '.go',
       '.groovy', '.hs', '.java', '.js', '.lua', '.m',
       '.module', '.php', '.pl', '.pm', '.py', '.rb', '.s', '.scala',
       '.sh', '.swift', '.tpl', '.twig'),size, null)) as avg_code_file_size
, avg(if( extension in ('.bat', '.c', '.cc', '.coffee', '.cpp', '.cs', '.cxx', '.go',
       '.groovy', '.hs', '.java', '.js', '.lua', '.m',
       '.module', '.php', '.pl', '.pm', '.py', '.rb', '.s', '.scala',
       '.sh', '.swift', '.tpl', '.twig')
       , if(size > 180000, 180000, size)
       , null)) as capped_avg_code_file_size

, sum(size) as sum_file_size
, sum(if(size > 180000, 180000, size)) as capped_sum_file_size
, sum(if( extension in ('.bat', '.c', '.cc', '.coffee', '.cpp', '.cs', '.cxx', '.go',
       '.groovy', '.hs', '.java', '.js', '.lua', '.m',
       '.module', '.php', '.pl', '.pm', '.py', '.rb', '.s', '.scala',
       '.sh', '.swift', '.tpl', '.twig'),size, null)) as sum_code_file_size
, sum(if( extension in ('.bat', '.c', '.cc', '.coffee', '.cpp', '.cs', '.cxx', '.go',
       '.groovy', '.hs', '.java', '.js', '.lua', '.m',
       '.module', '.php', '.pl', '.pm', '.py', '.rb', '.s', '.scala',
       '.sh', '.swift', '.tpl', '.twig')
       , if(size > 180000, 180000, size)
       , null)) as capped_sum_code_file_size

from
general.file_properties as fp
where
authors = 1
group by
author_email
;


update general.developer_profile as rp
set
avg_file_size = rl.avg_file_size
, capped_avg_file_size = rl.capped_avg_file_size
, avg_code_file_size = rl.avg_code_file_size
, capped_avg_code_file_size = rl.capped_avg_code_file_size

, sum_file_size = rl.sum_file_size
, capped_sum_file_size = rl.capped_sum_file_size
, sum_code_file_size = rl.sum_code_file_size
, capped_sum_code_file_size = rl.capped_sum_code_file_size

from
general.dev_length_properties as rl
where
rp.author_email = rl.author_email
;

drop table if exists general.dev_length_properties;

drop table if exists general.author_owned_files_revert_time;

create table
general.author_owned_files_revert_time
as
select
cf.Author_email
, avg(minutes_to_revert) as minutes_to_revert
, count(distinct cf.commit) as reverted_commits
from
general.file_properties as fp
join
general.commits_files as cf
on
fp.repo_name = cf.repo_name
and
fp.file = cf.file
join
general.reverted_commits as rc
on
cf.repo_name = rc.repo_name
and
cf.commit = rc.reverting_commit
where
authors = 1
group by
cf.Author_email
;


update general.developer_profile as rp
set
minutes_to_revert = aux.minutes_to_revert
, reverted_ratio = 1.0*aux.reverted_commits/rp.commits
from
general.author_owned_files_revert_time as aux
where
rp.Author_email = aux.Author_email
;

update general.developer_profile as rp
set minutes_to_revert = Null
where
minutes_to_revert = -1.0
;

update general.developer_profile as rp
set reverted_ratio = Null
where
reverted_ratio = -1.0
;

drop table if exists general.author_owned_files_revert_time;

drop table if exists general.dev_testing_pair_involvement;

create table general.dev_testing_pair_involvement
as
select
author_email
, avg(if(test_involved, 1,0) ) as testing_involved_prob
, if( sum(if(is_corrective, 1,0)) > 0
    , 1.0*sum(if(test_involved and is_corrective, 1,0) )/sum(if(is_corrective, 1,0))
    , null) as corrective_testing_involved_prob
, if( sum(if(is_refactor, 1,0)) > 0
    , 1.0*sum(if(test_involved and is_refactor, 1,0) )/sum(if(is_refactor, 1,0))
    , null) as refactor_testing_involved_prob
from
general.testing_pairs_commits
group by
author_email
;

update general.developer_profile as dp
set testing_involved_prob = aux.testing_involved_prob
, corrective_testing_involved_prob = aux.corrective_testing_involved_prob
, refactor_testing_involved_prob = aux.refactor_testing_involved_prob
from
general.dev_testing_pair_involvement as aux
where
dp.author_email = aux.author_email
;

drop table if exists general.dev_testing_pair_involvement;

