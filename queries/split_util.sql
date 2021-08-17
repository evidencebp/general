# Used to split entities to train, validation and test data sets.

CREATE OR REPLACE FUNCTION
general.bq_split
 (text string
 , salt string)
 RETURNS string
AS (
    case
        when substr(TO_HEX(md5(concat(text, salt))), 1,1) = '0'  then 'Validation'
        when substr(TO_HEX(md5(concat(text, salt))), 1,1) = '1'  then 'Test'
        else 'Train' end # Train

 )
;


WITH tab AS (
  SELECT  'tensorflow/tensorflow' AS repo_name
            , 'Train' as expected
    UNION ALL SELECT '3846masa/upload-gphotos'
                    , 'Validation'

    UNION ALL SELECT '3scale/echo-api'
                    , 'Test'

    UNION ALL SELECT null
                    , null
)
SELECT repo_name
, expected
, general.bq_split(repo_name, '') as actual
, general.bq_split(repo_name, '') = expected as pass
FROM tab as testing
;

CREATE OR REPLACE FUNCTION
general.bq_repo_split
 (repo_name string)
 RETURNS string
AS (
    general.bq_split(repo_name, '93enfuv')

 )
;


WITH tab AS (
  SELECT  'looker-open-source/kokoro-codelab-joeldodge' AS repo_name
            , 'Train' as expected
    UNION ALL SELECT 'kcivey/dc-ocf'
                    , 'Validation'

    UNION ALL SELECT 'kubeflow/pipelines'
                    , 'Test'

    UNION ALL SELECT null
                    , null
)
SELECT repo_name
, expected
, general.bq_repo_split(repo_name) as actual
, general.bq_repo_split(repo_name) = expected as pass
FROM tab as testing
;


CREATE OR REPLACE FUNCTION
general.bq_file_split
 (repo_name string
 , file string)
 RETURNS string
AS (
    general.bq_split(concat(repo_name
                              , file)
                        , 'hnu75j9')

 )
;


CREATE OR REPLACE FUNCTION
general.bq_file_pair_split
 (first_repo_name string
 , first_file string
 , second_repo_name string
 , second_file string
)
 RETURNS string
AS (
    general.bq_split(concat(first_repo_name
                              , first_file
                              , second_repo_name
                              , second_file)
                        , '7g5ide')

 )
;


CREATE OR REPLACE FUNCTION
general.bq_commit_split
 (commit string)
 RETURNS string
AS (
    general.bq_split(commit
                        , '6gd8h3')

 )
;
