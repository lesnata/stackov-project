import ast
from contextlib import closing
from datetime import datetime
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class PostgresMultipleUploadsOperator(BaseOperator):
    template_fields = ['top_answerers']

    def __init__(self, top_answerers,
                 postgres_conn_id="db_postgres",
                 *args, **kwargs):
        super(PostgresMultipleUploadsOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.top_answerers = top_answerers

    def execute(self, context):
        pg = PostgresHook(postgres_conn_id=self.postgres_conn_id, schema='postgres')
        with closing(pg.get_conn()) as conn:
            processed_answer = ast.literal_eval(self.top_answerers)
            cursor = conn.cursor()

            for i in processed_answer[0]['items']:
                user_id = i['user'].get("user_id")
                load_dts = datetime.now()
                display_name = i['user']['display_name'] or 'NULL'
                profile_image = i['user']['profile_image'] or 'NULL'
                user_type = i['user']['user_type']
                user_link = i['user']['link']
                score = i['score'] or 'NULL'
                post_count = i['post_count']
                accept_rate = i['user'].get('accept_rate', 'NULL')
                reputation = i['user']['reputation']
                rec_src = 'stackoverflow'

                request_1 = f"""
                INSERT INTO hub_user
                (user_id, load_dts, rec_src)
                VALUES
                ({user_id}, '{load_dts}', '{rec_src}')"""

                cursor.execute(request_1)

                request_2 = f"""
                INSERT INTO so_sat_user 
                (user_id_h_fk, load_dts, display_name, profile_image, user_type, user_link, rec_src, hash_diff) 
                VALUES 
                ({user_id}, '{load_dts}', '{display_name}', '{profile_image}', '{user_type}', '{user_link}', '{rec_src}', 
                (MD5({user_id} || '{load_dts}' || '{display_name}' || '{profile_image}' || '{user_type}' || '{user_link}' || '{rec_src}'))); 
                """
                cursor.execute(request_2)

                request_3 = f"""
                INSERT INTO so_sat_user_score 
                (user_id_h_fk, load_dts, score, accept_rate, post_count, reputation, rec_src, hash_diff) 
                VALUES 
                ({user_id}, '{load_dts}', {score}, {accept_rate}, {post_count}, {reputation}, '{rec_src}', 
                (MD5(CONCAT({user_id}, '{load_dts}', {score}, {accept_rate}, {post_count}, {reputation}, '{rec_src}'))));
            
                """
                cursor.execute(request_3)


