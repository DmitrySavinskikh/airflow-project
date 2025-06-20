import datetime
import logging

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


dag_default_args = {
    'owner': 'Rakhimova Guzel',
    'email': 'grrakhimova@edu.hse.ru',
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': datetime.timedelta(minutes=5),
    'depends_on_past': False
}

dag = DAG(
    default_args=dag_default_args,
    dag_id="dwh_team_21_load_ods_stg",
    schedule=None,
    start_date=datetime.datetime(2025, 1, 1),
    catchup=False,
    description="transformation data from ods to stg with seconds in timestamp",
    tags=["team_21"]
)

# create all stg tables
create_stg_tables = SQLExecuteQueryOperator(
    task_id='create_stg_tables',
    conn_id='con_dwh_2024_s086',
    sql="""
    -- Create airports table in STG (same structure as ODS)
    DROP TABLE IF EXISTS stg.airports_data;
    CREATE TABLE stg.airports_data (
        id INTEGER,
        ident TEXT,
        type TEXT,
        name TEXT,
        icao_code TEXT default 'Unknown',
        iata_code TEXT default 'Unknown'
    );
    
    -- Create weather tables in STG with transformed structure
    DROP TABLE IF EXISTS stg.weather_kgcc;
    CREATE TABLE stg.weather_kgcc (
        local_time TIMESTAMP,
        air_temperature decimal(4,1),
        pressure_ground decimal(5,1),
        pressure_sea decimal(5,1),
        humidity integer,
        mean_wind text,
        wind_speed integer,
        wind_speed_10_m text,
        description_rain text default 'no rain',
        descr_vision text,
        horiz_vision decimal(4,1)
    );

    DROP TABLE IF EXISTS stg.weather_kjac;
    CREATE TABLE stg.weather_kjac (
        local_time TIMESTAMP,
        air_temperature decimal(4,1),
        pressure_ground decimal(5,1),
        pressure_sea decimal(5,1),
        humidity integer,
        mean_wind text,
        wind_speed integer,
        wind_speed_10_m text,
        description_rain text default 'no rain',
        descr_vision text,
        horiz_vision decimal(4,1)
    );

    DROP TABLE IF EXISTS stg.weather_klar;
    CREATE TABLE stg.weather_klar (
        local_time TIMESTAMP,
        air_temperature decimal(4,1),
        pressure_ground decimal(5,1),
        pressure_sea decimal(5,1),
        humidity integer,
        mean_wind text,
        wind_speed integer,
        wind_speed_10_m text,
        description_rain text default 'no rain',
        descr_vision text,
        horiz_vision decimal(4,1)
    );

    DROP TABLE IF EXISTS stg.weather_kriw;
    CREATE TABLE stg.weather_kriw (
        local_time TIMESTAMP,
        air_temperature decimal(4,1),
        pressure_ground decimal(5,1),
        pressure_sea decimal(5,1),
        humidity integer,
        mean_wind text,
        wind_speed integer,
        wind_speed_10_m text,
        description_rain text default 'no rain',
        descr_vision text,
        horiz_vision decimal(4,1)
    );
    """,
    dag=dag
)

# task to load airports data
load_airports = SQLExecuteQueryOperator(
    task_id='load_airports_data',
    conn_id='con_dwh_2024_s086',
    sql="""
    INSERT INTO stg.airports_data (id, ident, type, name, icao_code, iata_code)
    SELECT DISTINCT
        id,
        ident,
        type,
        name,
        icao_code,
        iata_code
    FROM ods.airports_data;
    """,
    dag=dag
)

# tasks to load weather data with seconds in timestamp format
load_weather_kgcc = SQLExecuteQueryOperator(
    task_id='load_weather_kgcc',
    conn_id='con_dwh_2024_s086',
    sql="""
    INSERT INTO stg.weather_kgcc
    SELECT DISTINCT
        TO_TIMESTAMP(local_time, 'DD.MM.YYYY HH24:MI:SS') as local_time,
        T, P0, P, U, DD, Ff, ff10, WW, c, VV
    FROM ods.weather_kgcc;
    """,
    dag=dag
)

load_weather_kjac = SQLExecuteQueryOperator(
    task_id='load_weather_kjac',
    conn_id='con_dwh_2024_s086',
    sql="""
    INSERT INTO stg.weather_kjac
    SELECT DISTINCT
        TO_TIMESTAMP(local_time, 'DD.MM.YYYY HH24:MI:SS') as local_time,
        T, P0, P, U, DD, Ff, ff10, WW, c, VV
    FROM ods.weather_kjac;
    """,
    dag=dag
)

load_weather_klar = SQLExecuteQueryOperator(
    task_id='load_weather_klar',
    conn_id='con_dwh_2024_s086',
    sql="""
    INSERT INTO stg.weather_klar
    SELECT DISTINCT
        TO_TIMESTAMP(local_time, 'DD.MM.YYYY HH24:MI:SS') as local_time,
        T, P0, P, U, DD, Ff, ff10, WW, c, VV
    FROM ods.weather_klar;
    """,
    dag=dag
)

load_weather_kriw = SQLExecuteQueryOperator(
    task_id='load_weather_kriw',
    conn_id='con_dwh_2024_s086',
    sql="""
    INSERT INTO stg.weather_kriw
    SELECT DISTINCT
        TO_TIMESTAMP(local_time, 'DD.MM.YYYY HH24:MI:SS') as local_time,
        T, P0, P, U, DD, Ff, ff10, WW, c, VV
    FROM ods.weather_kriw;
    """,
    dag=dag
)

create_stg_tables >> load_airports >> load_weather_kgcc >> load_weather_kjac >> load_weather_klar >> load_weather_kriw
