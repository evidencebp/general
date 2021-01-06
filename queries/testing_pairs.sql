drop table if exists general.testing_pairs;

create table
general.testing_pairs
as
select
tested.repo_name as repo_name
, tested.file as test_file
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
