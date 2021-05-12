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