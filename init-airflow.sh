#!/bin/bash 

if [[ -z "${BUCKET}" ]]; then
    echo "Please set BUCKET to the name of the S3 bucket that contains the airflow files. Eg:"
    echo "BUCKET=s3-airflow-bucket $0"
    exit -1
fi

AWS_COMMAND="${AWS:=aws}"

#$AWS_COMMAND s3 cp requirements-airflow.txt s3://${BUCKET}/requirements.txt
$AWS_COMMAND s3 sync dags s3://${BUCKET}/dags
$AWS_COMMAND s3 sync src s3://${BUCKET}/dags/src --exclude __pycache__ 

