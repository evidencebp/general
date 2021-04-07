# Repo productivity metrics
drop table if exists general.repo_productivity_metrics;

create table
general.repo_productivity_metrics
as
select
repo_name
, count(distinct author_email) as developers
, count(distinct if(commits >= 12, author_email, null)) as involved_developers
, sum(if(commits >= 12, commits, 0)) as involved_developers_commits
, sum(if(commits < 500, commits, 500))  as developer_capped_commits
, sum(if(commits >= 12
            , if(commits < 500, commits, 500)
            , 0)) as involved_developers_capped_commits
from
general.developer_per_repo_profile
group by
repo_name
;


update general.repo_properties as rp
set
commits_per_developer = if(rp.authors > 0
                            , rp.commits/rp.authors
                            , null)
, involved_developers = aux.involved_developers
, involved_developers_commits = aux.involved_developers_commits
, commits_per_involved_developer = if(aux.involved_developers > 0
                                        , rp.commits/aux.involved_developers
                                        , null)
, developer_capped_commits = aux.developer_capped_commits
, capped_commits_per_developer = if(rp.authors > 0
                            , aux.developer_capped_commits/rp.authors
                            , null)
, involved_developers_capped_commits = aux.involved_developers_capped_commits
, capped_commits_per_involved_developer = if(aux.involved_developers > 0
                                        , aux.involved_developers_capped_commits/aux.involved_developers
                                        , null)
from
general.repo_productivity_metrics as aux
where
rp.repo_name = aux.repo_name
;


drop table if exists general.repo_productivity_metrics;

# Productivity metrics for repo_properties_per_year

drop table if exists general.repo_productivity_metrics_per_year;

create table
general.repo_productivity_metrics_per_year
as
select
repo_name
, year
, count(distinct author_email) as developers
, count(distinct if(commits >= 12, author_email, null)) as involved_developers
, sum(if(commits >= 12, commits, 0)) as involved_developers_commits
, sum(if(commits < 500, commits, 500))  as developer_capped_commits
, sum(if(commits >= 12
            , if(commits < 500, commits, 500)
            , 0)) as involved_developers_capped_commits
from
general.developer_per_repo_profile_per_year
group by
repo_name
, year
;

update general.repo_properties_per_year as rp
set
commits_per_developer = if(rp.authors > 0
                            , rp.commits/rp.authors
                            , null)
, involved_developers = aux.involved_developers
, involved_developers_commits = aux.involved_developers_commits
, commits_per_involved_developer = if(aux.involved_developers > 0
                                        , rp.commits/aux.involved_developers
                                        , null)
, developer_capped_commits = aux.developer_capped_commits
, capped_commits_per_developer = if(rp.authors > 0
                            , aux.developer_capped_commits/rp.authors
                            , null)
, involved_developers_capped_commits = aux.involved_developers_capped_commits
, capped_commits_per_involved_developer = if(aux.involved_developers > 0
                                        , aux.involved_developers_capped_commits/aux.involved_developers
                                        , null)
from
general.repo_productivity_metrics_per_year as aux
where
rp.repo_name = aux.repo_name
and
rp.year = aux.year
;

drop table if exists general.repo_productivity_metrics_per_year;
