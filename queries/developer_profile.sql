
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
#	\item Number of files edited in project
#	\item Number of files created in project
#    \item number of files owned in project

#	\item Number of commits in project
#	\item Percentage of self-commits to the entire project commits
#	\item Avg. CCP of files edited in project
#	\item Avg. CCP of files created in project
#	\item Number of adaptive commits
#	\item Corrective commits
#	\item Refactor commits
#	\item Percent of effective refactors
#	\item Use of tests (in general, in corrective commits, in adaptive commits)
#	\item Commit message linguistic characteristic (e.g., message length)
#	\item Days of week activity (e.g., number of days, working days was weekend)
#	\item Working hours (e.g., number of distinct hours).
#	\items Commits/distinct commits variation \cite{8952390} \idan{Consider more ideas from there}
# tests presence
# message length
# refactoring
# CCP in owned files
# CCP
# Duration
, avg(case when same_date_as_prev then duration else null end) as same_date_duration_avg
, count(distinct case when same_date_as_prev then commit else null end) as same_date_commits

, TIMESTAMP_DIFF(max(ec.commit_timestamp), min(ec.commit_timestamp), day) as commit_period
, count(distinct ec.commit_timestamp) as commit_days
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

# Duration in project
# Join time relative to project creation
# Number of commits in project
# Percentage of self-commits to the entire project commits
# Number of files edited in project
# Number of files created in project
# Avg. CCP of files created in project
# Number of contributed repositories of the developer
# Percent of effective refactors
# Use of tests (in general, in corrective commits, in adaptive commits)
# Commit message linguistic characteristic (e.g., message length)
# Days of week activity (e.g., number of days, working days was weekend)
# Working hours (e.g., number of distinct hours).
# Commits/distinct commits variation
# Developer Reputation Estimator (DRE)
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
where true
;

drop table if exists general.author_created_files;

create table
general.author_created_files
as
select
creator_email
, count(distinct file) as files
from
general.file_properties
group by
creator_email
;

update general.developer_profile as dp
set files_created = acf.files
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
from
general.file_properties
where
authors = 1
group by
Author_email
;

update general.developer_profile as dp
set files_owned = aof.files
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
from
general.commits_files
group by
author_email
;


update general.developer_profile as dp
set files_edited = aef.files
from
general.author_edited_files as aef
where
dp.author_email = aef.author_email
;
drop table if exists general.author_edited_files;

drop table if exists general.developer_per_repo_profile;

create table
general.developer_per_repo_profile
as
select
repo_name
, author_email
, max(author_name) as author_name
, count( distinct author_name) as names # For safety, see if the email has some names
, substr(author_email, STRPOS(author_email ,'@') + 1) as  author_email_domain
, max(if(substr(repo_name, 0, STRPOS(repo_name ,'/') -1) = author_name, 1, 0)) as owned_repository

, min(ec.commit_timestamp) as min_commit_timestamp
, max(ec.commit_timestamp) as max_commit_timestamp

, count(distinct commit) as commits
, min(commit) as min_commit
, max(commit) as max_commit

# tests presence
# message length
# refactoring
# CCP in owned files
# CCP
# Duration
, avg(case when same_date_as_prev then duration else null end) as same_date_duration_avg
, count(distinct case when same_date_as_prev then commit else null end) as same_date_commits

, TIMESTAMP_DIFF(max(ec.commit_timestamp), min(ec.commit_timestamp), day) as commit_period
, count(distinct ec.commit_timestamp) as commit_days
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

# Duration in project
# Join time relative to project creation
# Number of commits in project
# Percentage of self-commits to the entire project commits
# Number of files edited in project
# Number of files created in project
# Avg. CCP of files created in project
# Number of contributed repositories of the developer
# Percent of effective refactors
# Use of tests (in general, in corrective commits, in adaptive commits)
# Commit message linguistic characteristic (e.g., message length)
# Days of week activity (e.g., number of days, working days was weekend)
# Working hours (e.g., number of distinct hours).
# Commits/distinct commits variation
# Developer Reputation Estimator (DRE)
from
general.enhanced_commits as ec
#where
#commit_timestamp >= TIMESTAMP_ADD(current_timestamp(), INTERVAL -365 DAY)
group by
repo_name
, author_email
;


update general.developer_per_repo_profile
set days_entropy = - (case when Sunday_prob > 0 then Sunday_prob*log(Sunday_prob,2) else 0 end
                        + case when Monday_prob > 0 then Monday_prob*log(Monday_prob,2) else 0 end
                        + case when Tuesday_prob > 0 then Tuesday_prob*log(Tuesday_prob,2) else 0 end
                        + case when Wednesday_prob > 0 then Wednesday_prob*log(Wednesday_prob,2) else 0 end
                        + case when Thursday_prob > 0 then Thursday_prob*log(Thursday_prob,2) else 0 end
                        + case when Friday_prob > 0 then Friday_prob*log(Friday_prob,2) else 0 end
                        + case when Saturday_prob > 0 then Saturday_prob*log(Saturday_prob,2) else 0 end
)
where true
;

drop table if exists general.author_created_files_by_repo;

create table
general.author_created_files_by_repo
as
select
creator_email
, repo_name
, count(distinct file) as files
from
general.file_properties
group by
creator_email
, repo_name
;

drop table if exists general.developer_per_repo_profile_per_year;

create table
general.developer_per_repo_profile_per_year
as
select
repo_name
, extract(year from ec.commit_timestamp) as year
, author_email
, max(author_name) as author_name
, count( distinct author_name) as names # For safety, see if the email has some names
, substr(author_email, STRPOS(author_email ,'@') + 1) as  author_email_domain
, max(if(substr(repo_name, 0, STRPOS(repo_name ,'/') -1) = author_name, 1, 0)) as owned_repository

, min(ec.commit_timestamp) as min_commit_timestamp
, max(ec.commit_timestamp) as max_commit_timestamp

, count(distinct commit) as commits
, min(commit) as min_commit
, max(commit) as max_commit

# tests presence
# message length
# refactoring
# CCP in owned files
# CCP
# Duration
, avg(case when same_date_as_prev then duration else null end) as same_date_duration_avg
, count(distinct case when same_date_as_prev then commit else null end) as same_date_commits

, TIMESTAMP_DIFF(max(ec.commit_timestamp), min(ec.commit_timestamp), day) as commit_period
, count(distinct ec.commit_timestamp) as commit_days
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

# Duration in project
# Join time relative to project creation
# Number of commits in project
# Percentage of self-commits to the entire project commits
# Number of files edited in project
# Number of files created in project
# Avg. CCP of files created in project
# Number of contributed repositories of the developer
# Percent of effective refactors
# Use of tests (in general, in corrective commits, in adaptive commits)
# Commit message linguistic characteristic (e.g., message length)
# Days of week activity (e.g., number of days, working days was weekend)
# Working hours (e.g., number of distinct hours).
# Commits/distinct commits variation
# Developer Reputation Estimator (DRE)
from
general.enhanced_commits as ec
#where
#commit_timestamp >= TIMESTAMP_ADD(current_timestamp(), INTERVAL -365 DAY)
group by
repo_name
, year
, author_email
;


update general.developer_per_repo_profile_per_year
set days_entropy = - (case when Sunday_prob > 0 then Sunday_prob*log(Sunday_prob,2) else 0 end
                        + case when Monday_prob > 0 then Monday_prob*log(Monday_prob,2) else 0 end
                        + case when Tuesday_prob > 0 then Tuesday_prob*log(Tuesday_prob,2) else 0 end
                        + case when Wednesday_prob > 0 then Wednesday_prob*log(Wednesday_prob,2) else 0 end
                        + case when Thursday_prob > 0 then Thursday_prob*log(Thursday_prob,2) else 0 end
                        + case when Friday_prob > 0 then Friday_prob*log(Friday_prob,2) else 0 end
                        + case when Saturday_prob > 0 then Saturday_prob*log(Saturday_prob,2) else 0 end
)
where true
;
