import os
import ast
import json
import logging
from collections import namedtuple
from datetime import datetime, timedelta
from typing import List

from pandas import json_normalize
from botocore.exceptions import ClientError
from airflow.models import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pg_operator.pg_operator import PostgresMultipleUploadsOperator
from airflow.utils.task_group import TaskGroup

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


def _db_data_extractor(ti) -> List[namedtuple]:
        top_answerers = ti.xcom_pull(task_ids=['extracting_top_answerers'])
        clean_data_list = []
        for i in top_answerers[0]['items']:
            clean_data = dict()
            clean_data["user_bk"] = i['user']['user_id']
            clean_data["load_dts"] = str(datetime.now())
            clean_data["display_name"] = i['user']['display_name'] or None
            clean_data["profile_image"] = i['user']['profile_image'] or None
            clean_data["user_type"] = i['user']['user_type']
            clean_data["user_link"] = i['user']['link']
            clean_data["score"] = i['score'] or None
            clean_data["post_count"] = i['post_count']
            clean_data["accept_rate"] = i['user'].get('accept_rate', None)
            clean_data["reputation"] = i['user']['reputation']
            clean_data["rec_src"] = 'stackoverflow'
            clean_data_list.append(clean_data)
        logger.info(f"-------- DB DATA LIST OF {len(clean_data_list)} ITEMS READY FOR UPLOADING ---------")
        return clean_data_list


with DAG('stack_api', schedule_interval='@daily',
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

    db_data_extractor = PythonOperator(
        task_id="db_data_extractor",
        python_callable=_db_data_extractor
    )

    with TaskGroup('db_query_upload_data') as db_query_upload_data:
        hub_user = PostgresMultipleUploadsOperator(
            task_id='insert_hub_user',
            clean_data_list="{{ ti.xcom_pull(task_ids=['db_data_extractor']) }}",
            sql_insert_file="./dags/sql/STACKOV_INSERT_HUB_USER.sql",
        )

        sat_user = PostgresMultipleUploadsOperator(
            task_id='insert_sat_user',
            clean_data_list="{{ ti.xcom_pull(task_ids=['db_data_extractor']) }}",
            sql_insert_file="./dags/sql/STACKOV_INSERT_SAT_USER.sql"
        )

        sat_user_score = PostgresMultipleUploadsOperator(
            task_id='insert_sat_user_score',
            clean_data_list="{{ ti.xcom_pull(task_ids=['db_data_extractor']) }}",
            sql_insert_file="./dags/sql/STACKOV_INSERT_SAT_USER_SCORE.sql"
        )

    is_api_available >> extracting_top_answerers >> processing_answer >> creating_bucket >> uploading_to_s3 \
    >> db_query_create_table >> db_data_extractor >> db_query_upload_data
