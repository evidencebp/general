# general - testing pairs

drop table if exists general.testing_pairs;

create table
general.testing_pairs
as
select
tested.repo_name as repo_name
, tested.path as tested_file
, testing.path as testing_file
from
general.files as tested
join
general.files as testing
on
tested.repo_name = testing.repo_name
and
regexp_contains(lower(testing.path), 'test')
and
not regexp_contains(lower(tested.path), 'test')
and
REGEXP_REPLACE(lower(if(STRPOS(testing.path, '/') >= 0
    ,reverse(substr(reverse(testing.path), 0, STRPOS(reverse(testing.path), '/') ))
    , testing.path)), '(test(er|s|ting)?(bed|apps|data|suite)?|_|-|/|\\.)', '')
= REGEXP_REPLACE(lower(if(STRPOS(tested.path, '/') >= 0
    ,reverse(substr(reverse(tested.path), 0, STRPOS(reverse(tested.path), '/') ))
    , tested.path)), '(test(er|s|ting)?(bed|apps|data|suite)?|_|-|/|\\.)', '')
and
REGEXP_REPLACE(lower(testing.path), '([a-z0-9-]*test(er|s|ting)?[a-z0-9-]*|_|-|/)', '')
= REGEXP_REPLACE(lower(tested.path), '([a-z0-9-]*test(er|s|ting)?[a-z0-9-]*|_|-|/)', '')
# Might consider filtering general files which are not tests
#and
#lower(reverse(substr(reverse(testing.path), 0, STRPOS(reverse(testing.path), '/') ))) != '/__init__.py'
#and
#lower(reverse(substr(reverse(tested.path), 0, STRPOS(reverse(tested.path), '/') ))) != '/__init__.py'
;


WITH tab AS (
  SELECT  'timer_test.go' AS file
            , 'timergo' as expected
    UNION ALL SELECT 'ThrowingPushPromisesAsInputStreamCustom.java'
                    , 'throwingpushpromisesasinputstreamcustomjava'

    UNION ALL SELECT 'test_hyperrectdomain.cpp'
                    , 'hyperrectdomaincpp'

)
SELECT file
, REGEXP_REPLACE(lower(testing.file), '(test(er|s|ting)?(bed|apps|data|suite)?|_|-|/|\\.)', '') = expected as pass
, REGEXP_REPLACE(lower(testing.file), '(test(er|s|ting)?(bed|apps|data|suite)?|_|-|/|\\.)', '') as cannon_name
, expected
FROM tab as testing
;



WITH tab AS (
  SELECT  'dev/scripts/vendor/src/v.io/x/lib/timing/timer_test.go' AS file
            , 'devscriptsvendorsrcv.ioxlibtimingtimer.go' as expected
    UNION ALL SELECT 'test/jdk/java/net/httpclient/ThrowingPushPromisesAsInputStreamCustom.java'
                    , 'jdkjavanethttpclientthrowingpushpromisesasinputstreamcustom.java'

    UNION ALL SELECT 'tests/test_hyperrectdomain.cpp'
                    , 'hyperrectdomain.cpp'
    UNION ALL SELECT 'tests/benchmark/test-data/node_modules/lodash/_asciiWords.js'
                    , 'benchmarknodemoduleslodashasciiwords.js'
    UNION ALL SELECT 'tests/contrib/kubernetes/kubernetes_request_factory/test_kubernetes_request_factory.py'
                    , 'contribkuberneteskubernetesrequestfactorykubernetesrequestfactory.py'
    UNION ALL SELECT '/contrib/kubernetes/kubernetes_request_factory/kubernetes_request_factory.py'
                    , 'contribkuberneteskubernetesrequestfactorykubernetesrequestfactory.py'


)
SELECT file
, REGEXP_REPLACE(lower(testing.file), '([a-z0-9-]*test(er|s|ting)?[a-z0-9-]*|_|-|/)', '') = expected as pass
, REGEXP_REPLACE(lower(testing.file), '([a-z0-9-]*test(er|s|ting)?[a-z0-9-]*|_|-|/)', '') as cannon_name
, expected
FROM tab as testing
;

drop table if exists general.testing_pairs_commits;

create table general.testing_pairs_commits
as
select
cf.*
, cf_test_lookup.file is not null as test_involved
from
general.commits_files as cf
join
general.testing_pairs as pair
on
cf.repo_name = pair.repo_name
and
cf.file = pair.tested_file
left join
general.commits_files as cf_test_lookup
on
cf.repo_name = cf_test_lookup.repo_name
and
cf.commit = cf_test_lookup.commit
and
pair.testing_file = cf_test_lookup.file
;
