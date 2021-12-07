#!/bin/bash 

if [[ -z "${BUCKET}" ]]; then
    echo "Please set BUCKET to the name of the S3 bucket that contains the airflow files. Eg:"
    echo "BUCKET=s3-airflow-bucket $0"
    exit -1
fi

AWS_COMMAND="${AWS:=aws}"

$AWS_COMMAND s3 sync src s3://${BUCKET}/src
$AWS_COMMAND s3 sync dags s3://${BUCKET}/dags

