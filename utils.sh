#!/bin/bash 
function athena_query {
    local SQL=$1
    local ATHENA_QUERY_ID=$(aws athena start-query-execution \
                            --work-group dc_taxi_athena_workgroup \
                            --query 'QueryExecutionId' \
                            --output text \
                            --query-string "$SQL")
    echo $ATHENA_QUERY_ID
}
function athena_query_to_table {
    local SQL=$1
    local OUTPUT_QUERY=$2
    local MAX_ITEMS=${3:-10}

    local ATHENA_QUERY_ID=$(athena_query "$SQL")
    echo $ATHENA_QUERY_ID
    until aws athena get-query-execution \
      --query 'QueryExecution.Status.State' \
      --output text \
      --query-execution-id $ATHENA_QUERY_ID | grep -v "RUNNING";
    do
      printf '.'
      sleep 1; 
    done

    aws athena get-query-results \
      --max-items $MAX_ITEMS \
      --query-execution-id $ATHENA_QUERY_ID \
      --query $OUTPUT_QUERY \
      --output table
}

function athena_query_to_pandas {
    local SQL=$1
    local OUTPUT_QUERY=${2:-"ResultSet.Rows[*].[Data[].VarCharValue]"}
    local MAX_ITEMS=${3:-10}
    local TMP_JSON=${4:-"/tmp/awscli.json"}

    local ATHENA_QUERY_ID=$(athena_query "$SQL")
    echo $ATHENA_QUERY_ID
    until aws athena get-query-execution \
      --query 'QueryExecution.Status.State' \
      --output text \
      --query-execution-id $ATHENA_QUERY_ID | grep -v "RUNNING";
    do
      printf '.'
      sleep 1; 
    done

    aws athena get-query-results \
      --max-items $MAX_ITEMS \
      --query-execution-id $ATHENA_QUERY_ID \
      --query $OUTPUT_QUERY \
      --output json > $TMP_JSON      
}

function run_crawler {
  echo Attempting to run a crawler using:
  
  args=(  GLUE_CRAWLER_NAME 
          GLUE_DB_NAME 
          GLUE_TABLE_PREFIX         
          GLUE_TABLE_NAME 
          BUCKET_SRC_PATH 
          )

  for arg in "${args[@]}"
  do
    echo "  ${arg}=$(printenv ${arg})"
  done  
  
  aws glue delete-table --database-name dc_taxi_db --name dc_taxi_clean

  aws glue delete-crawler --name $GLUE_CRAWLER_NAME

  aws glue create-crawler --name $GLUE_CRAWLER_NAME \
    --database-name $GLUE_DB_NAME \
    --table-prefix $GLUE_TABLE_PREFIX \
    --role `aws iam get-role --role-name AWSGlueServiceRole-dc-taxi --query 'Role.Arn' --output text` --targets '{
    "S3Targets": [{
        "Path": "'$(
          echo $BUCKET_SRC_PATH
        )'"
      }]
  }'

  aws glue start-crawler --name $GLUE_CRAWLER_NAME

  printf "Waiting for crawler to finish..."
  while echo "$(aws glue get-crawler --name $GLUE_CRAWLER_NAME --query 'Crawler.State' --output text)" | grep -q -E "STARTING|RUNNING|STOPPING"; do
    sleep 30
    printf "..."
  done
  aws glue get-crawler --name $GLUE_CRAWLER_NAME --query 'Crawler.State' --output text

  printf "done\n"
}

function run_job {
  echo Attempting to run a job using:
  
  args=(  PYSPARK_SRC_NAME 
          PYSPARK_JOB_NAME 
          AWS_DEFAULT_REGION         
          BUCKET_ID 
          BUCKET_SRC_PATH 
          BUCKET_DST_PATH 
          BINS 
          SEED )

  for arg in "${args[@]}"
  do
    echo "  ${arg}=$(printenv ${arg})"
  done
  
  pyspark_run_job
}

function pyspark_run_job {

  aws s3 cp $PYSPARK_SRC_NAME s3://dc-taxi-$BUCKET_ID-$AWS_DEFAULT_REGION/glue/
  aws s3 ls s3://dc-taxi-$BUCKET_ID-$AWS_DEFAULT_REGION/glue/$PYSPARK_SRC_NAME
  
  aws glue delete-job --job-name $PYSPARK_JOB_NAME
  
  aws glue create-job \
    --name $PYSPARK_JOB_NAME \
    --role $(aws iam get-role --role-name AWSGlueServiceRole-dc-taxi --query 'Role.Arn' --output text) \
    --default-arguments '{"--TempDir":"s3://dc-taxi-'$BUCKET_ID'-'$AWS_DEFAULT_REGION'/glue/"}' \
    --command '{
      "ScriptLocation": "s3://dc-taxi-'$BUCKET_ID'-'$AWS_DEFAULT_REGION'/glue/'$PYSPARK_SRC_NAME'",
      "Name": "glueetl",
      "PythonVersion": "3"
    }' \
    --glue-version "2.0"

  aws glue start-job-run \
    --job-name $PYSPARK_JOB_NAME \
    --arguments='--BUCKET_SRC_PATH="'$(
        echo $BUCKET_SRC_PATH
      )'",
    --BUCKET_DST_PATH="'$(
        echo $BUCKET_DST_PATH
      )'",
    --SAMPLE_SIZE="'$(
        echo ${SAMPLE_SIZE:-1024}
      )'",    
    --BINS="'$(
        echo ${BINS:-1}
      )'",
    --SEED="'$(
        echo ${SEED:-42}
      )'"'

  printf "Waiting for the job to finish..."
  while echo "$(aws glue get-job-runs --job-name $PYSPARK_JOB_NAME --query 'JobRuns[0].JobRunState')" | grep -q -E "STARTING|RUNNING|STOPPING"; do
    sleep 30
    printf "..."
  done
  aws glue get-job-runs --job-name $PYSPARK_JOB_NAME --output text --query 'JobRuns[0].JobRunState'
}