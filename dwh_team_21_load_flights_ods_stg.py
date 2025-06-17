from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


S3_BUCKET = 'db01-content'
FLIGHT_PREFIX = 'flights/T_ONTIME_REPORTING'
POSTGRES_CONN_ID = 'con_dwh_2024_s086'
AWS_CONN_ID = 'object_storage_yc'
MONTHS_TO_LOAD = 6  # январь-июнь 2024

# даг для загрузки в stg
@dag(
    dag_id='dwh_team_21_load_flights_to_stg',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["team_21"]
)
def load_flights_to_stg_dag():

    @task
    def create_stg_table():
        sql = """
        DROP TABLE IF EXISTS stg.flights;
        CREATE TABLE stg.flights (
            flight_date DATE,
            carrier_code VARCHAR(10),
            origin_code VARCHAR(10),
            dest_code VARCHAR(10),
            scheduled_dep_time TIME,
            actual_dep_time TIME,
            dep_delay_min INTEGER,
            scheduled_arr_time TIME,
            actual_arr_time TIME,
            arr_delay_min INTEGER,
            distance INTEGER,
            weather_delay_min INTEGER,
            processed_dttm TIMESTAMPTZ
        );
        """
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        pg_hook.run(sql)

    @task
    def transform_and_load_to_stg():
        sql = """
        INSERT INTO stg.flights
        SELECT DISTINCT
            DATE(flight_datetime) AS flight_date,
            carrier_code,
            origin_code,
            dest_code,
            scheduled_dep_time,
            actual_dep_time,
            dep_delay_min,
            scheduled_arr_time,
            actual_arr_time,
            arr_delay_min,
            distance,
            weather_delay_min,
            NOW() AS processed_dttm
        FROM ods.flights
        WHERE flight_datetime BETWEEN '2024-01-01' AND '2024-06-30';
        """
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        pg_hook.run(sql)

    create_stg_table() >> transform_and_load_to_stg()

# инициализация дага
stg_dag = load_flights_to_stg_dag()