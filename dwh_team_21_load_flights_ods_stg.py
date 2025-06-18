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
    dag_id='dwh_team_21_load_flights_ods_stg',
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
            flight_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            carrier_flight_num VARCHAR(10),
            flight_date DATE,
            scheduled_dep_tm integer,
            actual_dep_tm integer,
            origin_airport_dk VARCHAR(10),
            dest_airport_dk VARCHAR(10),
            carrier_code VARCHAR(5),
            distance_miles INTEGER,
            dep_delay_min INTEGER,
            arr_delay_min INTEGER,
            carrier_delay_min INTEGER,
            weather_delay_min INTEGER,
            nas_delay_min INTEGER,
            security_delay_min INTEGER,
            late_aircraft_delay_min INTEGER,
            wheels_off_tm integer,
            wheels_on_tm INTEGER,
            cancelled_flag boolean,
            cancellation_code TEXT,
            processed_dttm TIMESTAMPTZ DEFAULT NOW()
        );
        """
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        pg_hook.run(sql)

    @task
    def transform_and_load_to_stg():
        sql = """
        INSERT INTO stg.flights (
            carrier_flight_num,
            flight_date,
            scheduled_dep_tm,
            actual_dep_tm,
            origin_airport_dk,
            dest_airport_dk,
            carrier_code,
            distance_miles,
            dep_delay_min,
            arr_delay_min,
            carrier_delay_min,
            weather_delay_min,
            nas_delay_min,
            security_delay_min,
            late_aircraft_delay_min,
            wheels_off_tm,
            wheels_on_tm,
            cancelled_flag,
            cancellation_code,
            processed_dttm)
        SELECT distinct	
            carrier_flight_num,
            to_date(flight_dt, 'MM/dd/yyyy hh:mi:ss a') AS flight_date,
            scheduled_dep_tm::integer,
            actual_dep_tm::integer,
            origin_code as origin_airport_dk,
            dest_code as dest_airport_dk,
            carrier_code,
            distance as distance_miles,
            dep_delay_min,
            arr_delay_min,
            carrier_delay_min,
            weather_delay_min,
            nas_delay_min,
            security_delay_min,
            late_aircraft_delay_min,
            wheels_off_tm::integer,
            wheels_on_tm::integer,
            case when cancelled_flg = 0 then false else true end,
            cancellation_code,
            NOW() AS processed_dttm
        FROM ods.flights
        WHERE to_date(flight_dt, 'MM/dd/yyyy hh:mi:ss a') BETWEEN '2024-01-01' AND '2024-06-30';
        """
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        pg_hook.run(sql)

    create_stg_table() >> transform_and_load_to_stg()

# инициализация дага
stg_dag = load_flights_to_stg_dag()