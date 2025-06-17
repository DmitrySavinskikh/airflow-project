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
    dag_id='dwh_team_21_load_flights_s3_ods',
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
            flight_dt TEXT,
            carrier_code TEXT,
            tail_num TEXT,
            carrier_flight_num TEXT,
            origin_code TEXT,
            origin_city_name TEXT,
            dest_code TEXT,
            dest_city_name TEXT,
            scheduled_dep_tm TEXT,
            actual_dep_tm TEXT,
            dep_delay_min float,
            dep_delay_group_num float,
            wheels_off_tm TEXT,
            wheels_on_tm TEXT,
            scheduled_arr_tm TEXT,
            actual_arr_tm TEXT,
            arr_delay_min float,
            arr_delay_group_num float,
            cancelled_flg float,
            cancellation_code TEXT,
            flights_cnt float,
            distance float,
            distance_group_num float,
            carrier_delay_min float,
            weather_delay_min float,
            nas_delay_min float,
            security_delay_min float,
            late_aircraft_delay_min float,
            processed_dttm timestamptz default now()
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
                            """COPY ods.flights (
                                year,
                                month,
                                flight_datetime,
                                carrier_code,
                                tail_num,
                                carrier_flight_num,
                                origin_code,
                                origin_city,
                                dest_code,
                                dest_city,
                                scheduled_dep_time,
                                actual_dep_time,
                                dep_delay_min,
                                wheels_off_tm,
                                wheels_on_tm,
                                scheduled_arr_tm,
                                actual_arr_tm,
                                arr_delay_min,
                                arr_delay_group_num,
                                cancelled_flg,
                                cancellation_code,
                                flights_cnt,
                                distance,
                                distance_group_num,
                                carrier_delay_min,
                                weather_delay_min,
                                nas_delay_min,
                                security_delay_min,
                                late_aircraft_delay_min
                                )
                                FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'""",
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