import ast
from contextlib import closing
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class PostgresMultipleUploadsOperator(BaseOperator):
    template_fields = ['clean_data_list']

    def __init__(self, clean_data_list,
                 postgres_conn_id="db_postgres",
                 *args, **kwargs):
        super(PostgresMultipleUploadsOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.clean_data_list = clean_data_list

    def execute(self, context):
        pg = PostgresHook(postgres_conn_id=self.postgres_conn_id, schema='postgres')
        json_data = ast.literal_eval(self.clean_data_list)
        with closing(pg.get_conn()) as conn:
            cursor = conn.cursor()
            for i in json_data[0]:

                request = f"""
                    WITH first_insert AS (
                        INSERT INTO hub_user
                        (user_bk, user_pk, load_dts, rec_src)
                        VALUES
                        ((MD5({i["user_pk"]} || '{i["rec_src"]}')), {i["user_pk"]}, '{i["load_dts"]}', '{i["rec_src"]}')
                        RETURNING user_pk
                    ), 
                    second_insert AS (
                        INSERT INTO so_sat_user 
                        (user_h_fk, load_dts, display_name, profile_image, user_type, user_link, rec_src, hash_diff) 
                        VALUES 
                        ( (SELECT user_pk from first_insert), '{i["load_dts"]}', '{i["display_name"]}', '{i["profile_image"]}', 
                        '{i["user_type"]}', '{i["user_link"]}', '{i["rec_src"]}', 
                        (MD5((SELECT user_pk from first_insert) || '{i["load_dts"]}' || '{i["display_name"]}' 
                        || '{i["profile_image"]}' || '{i["user_type"]}' || '{i["user_link"]}' || '{i["rec_src"]}')))
                    )
                    INSERT INTO so_sat_user_score 
                    (user_h_fk, load_dts, score, accept_rate, post_count, reputation, rec_src, hash_diff) 
                    VALUES 
                    ( (SELECT user_pk from first_insert), '{i["load_dts"]}', {i["score"]}, {i["accept_rate"]}, 
                    {i["post_count"]}, {i["reputation"]}, '{i["rec_src"]}', 
                    (MD5(CONCAT( (SELECT user_pk from first_insert), '{i["load_dts"]}', {i["score"]}, {i["accept_rate"]}, 
                    {i["post_count"]}, {i["reputation"]}, '{i["rec_src"]}')))
                    );
                
                """

                cursor.execute(request)

