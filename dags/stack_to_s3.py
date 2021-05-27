import os
import json
import logging
from datetime import datetime, timedelta
from pandas import json_normalize
from botocore.exceptions import ClientError
from airflow.models import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pg_operator.pg_operator import PostgresMultipleUploadsOperator

BUCKET_NAME = 'stackov-staging'
REGION = "eu-central-1"
FILE_PATH = '/tmp/processed_answerers.parquet.gzip'

logger = logging.getLogger("airflow.task")

default_args = {
    'start_date': datetime(2021, 5, 18),
    'owner': 'admin',
    'email': os.environ['USER_EMAIL'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=0.5)
}


def _processing_answer(ti):
    answer = ti.xcom_pull(task_ids=['extracting_top_answerers'])
    if not len(answer) or 'items' not in answer[0]:
        raise ValueError('StackOverflow Answer is empty')
    answerers = answer[0]["items"]
    output = json_normalize(answerers)
    output.to_parquet(FILE_PATH, engine='auto', compression='gzip', index=None)
    return answerers


def _creating_bucket():
    s3_hook = S3Hook(aws_conn_id="aws_conn", region_name=REGION)
    if s3_hook.check_for_bucket(bucket_name=BUCKET_NAME):
        logger.info("--------BUCKET ALREADY EXISTS---------")
        return True
    else:
        try:
            s3_hook.create_bucket(bucket_name=BUCKET_NAME, region_name=REGION)
            logger.info("--------BUCKET CREATED---------")
        except ClientError as e:
            logger.error(e)
            raise ValueError("----------CAN'T CREATE BUCKET------------")


def _uploading_to_s3():
    key = f'results_{datetime.now()}'
    s3_hook = S3Hook(aws_conn_id="aws_conn", region_name=REGION)
    try:
        s3_hook.load_file(filename=FILE_PATH, key=key, bucket_name=BUCKET_NAME,
                          replace=False)
        logger.info("--------FILE UPLOADED TO S3---------")
    except ClientError as e:
        logger.error(e)
        raise ValueError("----------CAN'T UPLOAD FILE------------")


with DAG('stack_to_s3', schedule_interval='@daily',
         default_args=default_args, catchup=False) as dag:

    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='stack_api',
        endpoint='2.2/tags/airflow/top-answerers/month?pagesize=20&site=stackoverflow'
    )

    extracting_top_answerers = SimpleHttpOperator(
        task_id='extracting_top_answerers',
        http_conn_id='stack_api',
        endpoint='2.2/tags/airflow/top-answerers/month?pagesize=20&site=stackoverflow',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

    processing_answer = PythonOperator(
        task_id='processing_answer',
        python_callable=_processing_answer
    )

    creating_bucket = PythonOperator(
        task_id='creating_bucket',
        python_callable=_creating_bucket
    )

    uploading_to_s3 = PythonOperator(
        task_id='uploading_to_s3',
        python_callable=_uploading_to_s3
    )

    db_query_create_table = PostgresOperator(
        task_id="db_query_create_table",
        postgres_conn_id="db_postgres",
        sql='sql/CREATE_TABLE.sql'
    )

    db_query_upload_data = PostgresMultipleUploadsOperator(
        task_id='db_query_upload_data',
        top_answerers="{{ ti.xcom_pull(task_ids=['extracting_top_answerers']) }}"
    )

    is_api_available >> extracting_top_answerers >> processing_answer >> creating_bucket >> uploading_to_s3 \
    >> db_query_create_table >> db_query_upload_data
