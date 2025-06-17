from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

# Общие настройки
S3_BUCKET = 'db01-content'
FLIGHT_PREFIX = 'flights/T_ONTIME_REPORTING'
POSTGRES_CONN_ID = 'con_dwh_2024_s086'
AWS_CONN_ID = 'object_storage_yc'
MONTHS_TO_LOAD = 6  # Январь-Июнь 2024

# DAG для загрузки в ODS
@dag(
    dag_id='dwh_team_21_load_flights_to_ods',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['team_21']
)
def load_flights_to_ods_dag():

    @task
    def create_ods_table():
        sql = """
        DROP TABLE IF EXISTS ods.flights;
        CREATE TABLE ods.flights (
            year INTEGER,
            month INTEGER,
            flight_datetime TIMESTAMP,
            carrier_code VARCHAR(10),
            tail_num VARCHAR(10),
            flight_number VARCHAR(10),
            origin_code VARCHAR(10),
            origin_city TEXT,
            dest_code VARCHAR(10),
            dest_city TEXT,
            scheduled_dep_time TIME,
            actual_dep_time TIME,
            dep_delay_min INTEGER,
            scheduled_arr_time TIME,
            actual_arr_time TIME,
            arr_delay_min INTEGER,
            cancelled_flag BOOLEAN,
            distance INTEGER,
            weather_delay_min INTEGER,
            processed_dttm TIMESTAMPTZ DEFAULT now()
        );
        """
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        pg_hook.run(sql)

    @task
    def load_to_ods():
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        for month in range(1, MONTHS_TO_LOAD + 1):
            month_str = f"{month:02d}"
            key = f"{FLIGHT_PREFIX}-2024-{month_str}.csv"
            
            try:
                file_name = s3_hook.download_file(key=key, bucket_name=S3_BUCKET)
                
                if file_name:
                    conn = pg_hook.get_conn()
                    cursor = conn.cursor()
                    
                    with open(file_name, 'r') as file:
                        cursor.copy_expert(
                            """COPY ods.flights FROM STDIN WITH (
                                FORMAT CSV,
                                HEADER,
                                DELIMITER ',',
                                QUOTE '"',
                                NULL ''
                            )""",
                            file
                        )
                    conn.commit()
                    logging.info(f"Loaded {key} to ODS")
                    
            except Exception as e:
                logging.error(f"Error loading {key}: {str(e)}")
                continue

    create_ods_table() >> load_to_ods()

# инициализация дага
ods_dag = load_flights_to_ods_dag()