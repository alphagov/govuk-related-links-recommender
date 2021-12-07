# Running related links in Airflow

The source files need to be copied to the S3 bucket used by Airflow, using:

```
AWS="gds aws [your-aws-role] aws" BUCKET="your-airflow-bucket-name] ./init-airflow.sh
```

It is also necessary to set the following
[variables](https://airflow.apache.org/docs/apache-airflow/stable/howto/variable.html)
in your Airflow environment

- `mongodb_uri`: the location of the content store, eg. `mongodb://ip-1-2-3-4.eu-west-1.compute.internal/content_store`
