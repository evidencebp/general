drop table if exists general.testing_pairs;

create table
general.testing_pairs
as
select
tested.repo_name as repo_name
, tested.file as tested_file
, testing.file as testing_file
from
general.file_properties as tested
join
general.file_properties as testing
on
tested.repo_name = testing.repo_name
and
testing.is_test
and
not tested.is_test
and
REGEXP_REPLACE(lower(if(STRPOS(testing.file, '/') >= 0
    ,reverse(substr(reverse(testing.file), 0, STRPOS(reverse(testing.file), '/') ))
    , testing.file)), '(test(er|s|ting)?(bed|apps|data|suite)?|_|-|/|\\.)', '')
= REGEXP_REPLACE(lower(if(STRPOS(tested.file, '/') >= 0
    ,reverse(substr(reverse(tested.file), 0, STRPOS(reverse(tested.file), '/') ))
    , tested.file)), '(test(er|s|ting)?(bed|apps|data|suite)?|_|-|/|\\.)', '')

and
REGEXP_REPLACE(lower(testing.file), '([a-z0-9-]*test(er|s|ting)?[a-z0-9-]*|_|-|/)', '')
= REGEXP_REPLACE(lower(tested.file), '([a-z0-9-]*test(er|s|ting)?[a-z0-9-]*|_|-|/)', '')
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

# Hunting false negatives
select
testing.repo_name
, testing.file
, REGEXP_REPLACE(lower(testing.file), '([a-z0-9-]*test(er|s|ting)?[a-z0-9-]*|_|-|/)', '') as cannon_name
from
general.file_properties as testing
left join
general.testing_pairs as pairs
on
testing.repo_name = pairs.repo_name
and
testing.file = pairs.testing_file
where
testing.is_test
and
testing.code_extension
and
pairs.testing_file is null
;

select
count(distinct concat(testing.repo_name, testing.file)) as testing_files
, count(distinct if(pairs.testing_file is null, null, concat(testing.repo_name, testing.file))) as matched_testing_files
, 1.0*count(distinct if(pairs.testing_file is null, null, concat(testing.repo_name, testing.file)))
    /count(distinct concat(testing.repo_name, testing.file)) as match_ratio
from
general.file_properties as testing
left join
general.testing_pairs as pairs
on
testing.repo_name = pairs.repo_name
and
testing.file = pairs.testing_file
where
testing.is_test
and
testing.code_extension
;

select
testing.repo_name as repo_name
, count(distinct concat(testing.repo_name, testing.file)) as testing_files
, count(distinct if(pairs.testing_file is null, null, concat(testing.repo_name, testing.file))) as matched_testing_files
, 1.0*count(distinct if(pairs.testing_file is null, null, concat(testing.repo_name, testing.file)))
    /count(distinct concat(testing.repo_name, testing.file)) as match_ratio
from
general.file_properties as testing
left join
general.testing_pairs as pairs
on
testing.repo_name = pairs.repo_name
and
testing.file = pairs.testing_file
where
testing.is_test
and
testing.code_extension
group by
testing.repo_name
order by
match_ratio
;


