from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

logger = logging.getLogger(__name__)

DAG_ID = 'dwh_team_21_load_flights_stg_dds'
PG_CONN_ID = 'con_dwh_2024_s086'
TEAM_AIRPORTS = ('JAC', 'LAR', 'GCC', 'RIW')
MAX_RETRIES = 3

@dag(
    dag_id=DAG_ID,
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2024, 1, 1),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval=None,
    catchup=False,
    tags=['team_21'],
)
def flights_to_dds_dag():

    @task
    def create_dds_schema():
        sql = """       
        DROP TABLE IF EXISTS dds.completed_flights;
        CREATE TABLE dds.completed_flights (
            flight_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            carrier_flight_num VARCHAR(10),
            flight_date DATE,
            scheduled_dep_tm TIMESTAMPTZ,
            actual_dep_tm TIMESTAMPTZ,
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
            late_aircraft_min INTEGER,
            wheels_off_tm TIMESTAMPTZ,
            wheels_on_tm TIMESTAMPTZ,
            processed_dttm TIMESTAMPTZ DEFAULT NOW(),
            CONSTRAINT uniq_flight UNIQUE (carrier_flight_num, scheduled_dep_tm, origin_airport_dk)
        );

        DROP TABLE IF EXISTS dds.cancelled_flights;
        CREATE TABLE dds.cancelled_flights (
            cancellation_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            carrier_flight_num VARCHAR(10),
            scheduled_dep_tm TIMESTAMP,
            origin_airport_dk VARCHAR(10),
            dest_airport_dk VARCHAR(10),
            carrier_code VARCHAR(5),
            cancellation_code CHAR(1),
            cancellation_reason TEXT GENERATED ALWAYS AS (
                CASE cancellation_code
                    WHEN 'A' THEN 'Carrier'
                    WHEN 'B' THEN 'Weather'
                    WHEN 'C' THEN 'National Air System'
                    WHEN 'D' THEN 'Security'
                    ELSE 'Unknown'
                END
            ) STORED,
            processed_dttm TIMESTAMPTZ DEFAULT NOW(),
            CONSTRAINT uniq_cancellation UNIQUE (carrier_flight_num, scheduled_dep_tm, origin_airport_dk, origin_airport_dk)
        );
        """
        hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
        hook.run(sql)

    @task
    def load_completed_flights():
        sql = """
        INSERT INTO dds.completed_flights (
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
            late_aircraft_min,
            wheels_off_tm, 
            wheels_on_tm
        )
        SELECT
            f.carrier_flight_num,
            f.flight_date::DATE,
            concat(flight_date::text, 
                        ' ', 
                        case when length(div(scheduled_dep_tm, 100)::text) = 1 then concat('0', div(scheduled_dep_tm, 100)::text) else div(scheduled_dep_tm, 100)::text end, 
                        ':', 
                        case when length(mod(scheduled_dep_tm, 100)::text) = 1 then '00' else mod(scheduled_dep_tm, 100)::text end,
                        ':00.000'
                )::timestamp as sched_dttm_local,
            concat(flight_date::text, 
                        ' ', 
                        case when length(div(actual_dep_tm, 100)::text) = 1 then concat('0', div(actual_dep_tm, 100)::text) else div(actual_dep_tm, 100)::text end, 
                        ':', 
                        case when length(mod(actual_dep_tm, 100)::text) = 1 then '00' else mod(actual_dep_tm, 100)::text end,
                        ':00.000'
                )::timestamp as actual_dttm_local,
            f.origin_airport_dk,
            f.dest_airport_dk,
            f.carrier_code,
            f.distance_miles,
            f.dep_delay_min,
            f.arr_delay_min,
            COALESCE(f.carrier_delay_min, 0),
            COALESCE(f.weather_delay_min, 0),
            COALESCE(f.nas_delay_min, 0),
            COALESCE(f.security_delay_min, 0),
            COALESCE(f.late_aircraft_delay_min, 0),
            concat(flight_date::text, 
                ' ', 
                case when length(div(wheels_off_tm, 100)::text) = 1 then concat('0', div(wheels_off_tm, 100)::text) else div(wheels_off_tm, 100)::text end, 
                ':', 
                case when length(mod(wheels_off_tm, 100)::text) = 1 then '00' else mod(wheels_off_tm, 100)::text end,
                ':00.000'
            )::timestamp as wheels_off_dttm,
            concat(flight_date::text, 
                ' ', 
                case when length(div(wheels_on_tm, 100)::text) = 1 then concat('0', div(wheels_on_tm, 100)::text) else div(wheels_on_tm, 100)::text end, 
                ':', 
                case when length(mod(wheels_on_tm, 100)::text) = 1 then '00' else mod(wheels_on_tm, 100)::text end,
                ':00.000'
            )::timestamp as wheels_on_dttm
        FROM stg.flights f
        LEFT JOIN dds_dict.dict_airports a ON a.iata_code = f.origin_airport_dk
        WHERE f.cancelled_flag is false
            AND f.origin_airport_dk IN ('JAC', 'LAR', 'GCC', 'RIW');
        """
        hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
        hook.run(sql, parameters=(tuple(TEAM_AIRPORTS),))

    @task
    def load_cancelled_flights():
        sql = """
        INSERT INTO dds.cancelled_flights (
            carrier_flight_num, 
            scheduled_dep_tm,
            origin_airport_dk, 
            dest_airport_dk,
            carrier_code, 
            cancellation_code
        )
        SELECT
            f.carrier_flight_number,
            (f.flight_dt || ' ' || f.scheduled_dep_tm)::timestamp AT TIME ZONE COALESCE(a.timezone, 'UTC'),
            f.origin_code,
            f.dest_code,
            f.carrier_code,
            f.cancellation_code
        FROM stg.flights f
        LEFT JOIN dds_dict.dict_airports a ON a.iata_code = f.origin_code
        WHERE f.cancelled_flg = 'Y'
            AND f.origin_code IN ('JAC', 'LAR', 'GCC', 'RIW')
            ON CONFLICT (carrier_flight_num, scheduled_dep_tm, origin_airport_dk) 
            DO NOTHING;
        """
        hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
        hook.run(sql, parameters=(tuple(TEAM_AIRPORTS),))

    create_dds_schema() >> load_completed_flights() >> load_cancelled_flights()

flights_dag = flights_to_dds_dag()