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

start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

create_stg_tables = SQLExecuteQueryOperator(
    task_id='create_stg_tables',
    conn_id='con_dwh_2024_s086',
    sql="""
    -- Create airports table in STG (same structure as ODS)
    CREATE TABLE IF NOT EXISTS stg.airports_data (
        id INTEGER default null,
        ident TEXT default null,
        type TEXT default null,
        name TEXT default null,
        latitude_deg DECIMAL(10, 6) default null,
        longitude_deg DECIMAL(10, 6) default null,
        elevation_ft INTEGER default null,
        continent TEXT default null,
        iso_country TEXT default null,
        iso_region TEXT default null,
        municipality TEXT default null,
        scheduled_service TEXT default null,
        icao_code TEXT default null,
        iata_code TEXT default null,
        gps_code TEXT default null,
        local_code TEXT default null,
        home_link TEXT default null,
        wikipedia_link TEXT default null,
        keywords TEXT default null
    );
    
    -- Create weather tables in STG with transformed structure
    CREATE TABLE IF NOT EXISTS stg.weather_kgcc (
        local_time TIMESTAMP default null,
        air_temperature decimal(4,1) default null,
        pressure_groud decimal(5,1) default null,
        pressure_sea decimal(5,1) default null,
        humidity integer default null,
        mean_wind text default null,
        wind_speed integer default null,
        wind_speed_10_m text default null,
        description_rain text default null,
        descr_vision text default null,
        horiz_vision decimal(4,1) default null
    );

    CREATE TABLE IF NOT EXISTS stg.weather_kjac (
        local_time TIMESTAMP default null,
        air_temperature decimal(4,1) default null,
        pressure_groud decimal(5,1) default null,
        pressure_sea decimal(5,1) default null,
        humidity integer default null,
        mean_wind text default null,
        wind_speed integer default null,
        wind_speed_10_m text default null,
        description_rain text default null,
        descr_vision text default null,
        horiz_vision decimal(4,1) default null
    );

    CREATE TABLE IF NOT EXISTS stg.weather_klar (
        local_time TIMESTAMP default null,
        air_temperature decimal(4,1) default null,
        pressure_groud decimal(5,1) default null,
        pressure_sea decimal(5,1) default null,
        humidity integer default null,
        mean_wind text default null,
        wind_speed integer default null,
        wind_speed_10_m text default null,
        description_rain text default null,
        descr_vision text default null,
        horiz_vision decimal(4,1) default null
    );

    CREATE TABLE IF NOT EXISTS stg.weather_kriw (
        local_time TIMESTAMP default null,
        air_temperature decimal(4,1) default null,
        pressure_groud decimal(5,1) default null,
        pressure_sea decimal(5,1) default null,
        humidity integer default null,
        mean_wind text default null,
        wind_speed integer default null,
        wind_speed_10_m text default null,
        description_rain text default null,
        descr_vision text default null,
        horiz_vision decimal(4,1) default null
    );
    """,
    dag=dag
)

# Task to load airports data (unchanged)
load_airports = SQLExecuteQueryOperator(
    task_id='load_airports_data',
    conn_id='con_dwh_2024_s086',
    sql="""
    TRUNCATE TABLE stg.airports_data;
    INSERT INTO stg.airports_data
    SELECT DISTINCT
        id,
        ident,
        type,
        name,
        latitude_deg,
        longitude_deg,
        elevation_ft,
        continent,
        iso_country,
        iso_region,
        municipality,
        scheduled_service,
        icao_code,
        iata_code,
        gps_code,
        local_code,
        home_link,
        wikipedia_link,
        keywords
    FROM ods.airports_data;
    """,
    dag=dag
)

# Updated tasks to load weather data with seconds in timestamp format
load_weather_kgcc = SQLExecuteQueryOperator(
    task_id='load_weather_kgcc',
    conn_id='con_dwh_2024_s086',
    sql="""
    TRUNCATE TABLE stg.weather_kgcc;
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
    TRUNCATE TABLE stg.weather_kjac;
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
    TRUNCATE TABLE stg.weather_klar;
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
    TRUNCATE TABLE stg.weather_kriw;
    INSERT INTO stg.weather_kriw
    SELECT DISTINCT
        TO_TIMESTAMP(local_time, 'DD.MM.YYYY HH24:MI:SS') as local_time,
        T, P0, P, U, DD, Ff, ff10, WW, c, VV
    FROM ods.weather_kriw;
    """,
    dag=dag
)

start >> create_stg_tables >> load_weather_kgcc >> load_weather_kjac >> load_weather_klar >> load_weather_kriw >> end
