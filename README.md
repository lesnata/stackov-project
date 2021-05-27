## StackOverflow API to S3 project
* [Description](#description)
* [Requirements](#requirements)
* [ENV variables](#env-variables)
* [Run the project](#run-the-project)

## Description
'Stack_to_s3' DAG pulls Top-20 answerers for the last month from StackOverflow API everyday, 
stores at '/tmp/processed_answerers.parquet.gzip' file and pushes to S3 bucket. 
Also, the same data is stored in PostgresDB with Data Vault model. 

 
## Requirements 
* Set up Airflow AWS connection: Access key ID & Secret access key required, conn_id=aws_conn
* Set up Airflow HTTP connection: Host https://api.stackexchange.com/, conn_id=stack_api
* Set up Airflow Postgres connection: conn_id=db_postgres


## ENV variables
```
AIRFLOW_UID
AIRFLOW_GID
USER_EMAIL
```


## Run the project:
```
# initial Airflow set up
docker-compose up airflow-init
# run Airflow
docker-compose up
```

