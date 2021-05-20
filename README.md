## StackOverflow API to S3 project
* [Description](#description)
* [Requirements](#requirements)
* [ENV variables](#env-variables)
* [Run the project](#run-the-project)

## Description
'Stack_to_s3' DAG pulls Top-20 answerers for the last month from StackOverflow API everyday, 
stores at '/tmp/processed_answerers.csv' file and pushes to S3 bucket in gzip format

 
## Requirements 
* Set up Airflow AWS connection: Access key ID & Secret access key required
* Set up Airflow HTTP connection: Host https://api.stackexchange.com/


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

