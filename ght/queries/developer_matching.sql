# This code matches developers from the general and general_ght schemas.
# The matching is done by commits appearing in both schemas, and duplication removal for safety.

drop table if exists general_ght.developer_matching;

create table
general_ght.developer_matching
as
select
gc.author_id
, ec.author_email
, count(distinct gc.sha) as matching_commits
from
general_ght.commits as gc
join
general.enhanced_commits as ec
on
gc.sha = ec.commit
group by
gc.author_id
, ec. author_email
;

drop table if exists general_ght.developer_matching_duplicated_author_id;

create table
general_ght.developer_matching_duplicated_author_id
as
select
author_id
, count(distinct author_email) as emails
from
general_ght.developer_matching
group by
author_id
having
count(distinct author_email) > 1
;


DELETE general_ght.developer_matching m
WHERE m.author_id IN (SELECT author_id from general_ght.developer_matching_duplicated_author_id)
;

drop table if exists general_ght.developer_matching_duplicated_author_email;


drop table if exists general_ght.developer_matching_duplicated_author_email;

create table
general_ght.developer_matching_duplicated_author_email
as
select
author_email
, count(distinct author_id) as ids
from
general_ght.developer_matching
group by
author_email
having
count(distinct author_id) > 1
;


DELETE general_ght.developer_matching m
WHERE m.author_email IN (SELECT author_email from general_ght.developer_matching_duplicated_author_email)
;

drop table if exists general_ght.developer_matching_duplicated_author_email;
