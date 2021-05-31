import ast
from contextlib import closing
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class PostgresMultipleUploadsOperator(BaseOperator):
    template_fields = ['clean_data_list', 'sql_file_path_hub_user',
                       'sql_file_path_sat_user', 'sql_file_path_sat_user_score']

    def __init__(self, clean_data_list,
                 sql_file_path_hub_user,
                 sql_file_path_sat_user,
                 sql_file_path_sat_user_score,
                 postgres_conn_id="db_postgres",
                 *args, **kwargs):
        super(PostgresMultipleUploadsOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.clean_data_list = clean_data_list
        self.sql_file_path_hub_user = sql_file_path_hub_user
        self.sql_file_path_sat_user = sql_file_path_sat_user
        self.sql_file_path_sat_user_score = sql_file_path_sat_user_score

    def execute(self, context):
        pg = PostgresHook(postgres_conn_id=self.postgres_conn_id, schema='postgres')
        json_data = ast.literal_eval(self.clean_data_list)
        with closing(pg.get_conn()) as conn:
            cursor = conn.cursor()
            for i in json_data[0]:
                data = {'user_bk': i["user_bk"], 'rec_src': i["rec_src"], 'load_dts': i["load_dts"],
                        'display_name': i["display_name"], 'profile_image': i["profile_image"],
                        'user_type': i["user_type"], 'user_link': i["user_link"],
                        'score': i["score"], 'accept_rate': i["accept_rate"],
                        'post_count': i["post_count"], 'reputation': i["reputation"]
                        }
                sql_file_hub_user = open(self.sql_file_path_hub_user, 'r')
                sql_file_path_sat_user = open(self.sql_file_path_sat_user, 'r')
                sql_file_path_sat_user_score = open(self.sql_file_path_sat_user_score, 'r')

                cursor.execute(sql_file_hub_user.read(), data)
                cursor.execute(sql_file_path_sat_user.read(), data)
                cursor.execute(sql_file_path_sat_user_score.read(), data)

