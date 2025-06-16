import datetime
import logging

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


dag_default_args = {
    'owner': 'Savinskikh Dmitry',
    'email': 'dasavinskikh@edu.hse.ru',
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': datetime.timedelta(minutes=5)
}

dag = DAG(
    default_args=dag_default_args,
    dag_id="dwh_team_21_load_stg_dds",
    schedule=None,
    start_date=datetime.datetime(2025, 1, 1),
    catchup=False,
    description="extraction data from stg to dds",
    tags=["team_21"]
)

start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

create_dds_airports = SQLExecuteQueryOperator(
    task_id='create_dds_airports',
    conn_id='con_dwh_2024_s086',
    sql="""
    -- Create airports table in DDS (only useful fields)
    CREATE TABLE IF NOT EXISTS dds_dict.airports_data (
        id INTEGER default null,
        ident TEXT default null,
        type TEXT default null,
        name TEXT default null,
        icao_code TEXT default null,
        iata_code TEXT default null
    );
    """,
    dag=dag
)

load_dds_airports = SQLExecuteQueryOperator(
    task_id='load_dds_airports',
    conn_id='con_dwh_2024_s086',
    sql="""
        INSERT INTO dds_dict.airports_data
        SELECT 
            id,
            ident,
            type,
            name,
            icao_code,
            iata_code
        FROM stg.airports_data
    """,
    dag=dag
)
    
create_dds_weather_kgcc = SQLExecuteQueryOperator(
    task_id='create_dds_weather_kgcc',
    conn_id='con_dwh_2024_s086',
    sql="""
    -- Create weather table in DDS (added scd2); at KGCC airport
    CREATE TABLE IF NOT EXISTS dds.h2_weather_kgcc (
        temperature_key TEXT PRIMARY KEY,
        air_temperature INTEGER,
        pressure_ground DECIMAL(7,2),
        pressure_sea DECIMAL(7,2),
        humidity INTEGER,
        mean_wind TEXT,
        wind_speed INTEGER,
        wind_speed_10_m INTEGER,
        description_rain VARCHAR(100),
        descr_vision VARCHAR(100),
        horiz_vision INTEGER,
        effective_from_dttm TIMESTAMP NOT NULL,
        effective_to_dttm TIMESTAMP DEFAULT '9999-12-31 23:59:59',
        is_current_flg BOOLEAN DEFAULT TRUE,
        is_deleted_flg BOOLEAN DEFAULT FALSE,
        processed_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    dag=dag
)

create_dds_weather_kjac = SQLExecuteQueryOperator(
    task_id='create_dds_weather_kjac',
    conn_id='con_dwh_2024_s086',
    sql="""
    -- Create weather table in DDS (added scd2); at KJAR airport
    CREATE TABLE IF NOT EXISTS dds.h2_weather_kjac (
        temperature_key TEXT PRIMARY KEY,
        air_temperature INTEGER,
        pressure_ground DECIMAL(7,2),
        pressure_sea DECIMAL(7,2),
        humidity INTEGER,
        mean_wind TEXT,
        wind_speed INTEGER,
        wind_speed_10_m INTEGER,
        description_rain VARCHAR(100),
        descr_vision VARCHAR(100),
        horiz_vision INTEGER,
        effective_from_dttm TIMESTAMP NOT NULL,
        effective_to_dttm TIMESTAMP DEFAULT '9999-12-31 23:59:59',
        is_current_flg BOOLEAN DEFAULT TRUE,
        is_deleted_flg BOOLEAN DEFAULT FALSE,
        processed_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    dag=dag
)

create_dds_weather_klar = SQLExecuteQueryOperator(
    task_id='create_dds_weather_klar',
    conn_id='con_dwh_2024_s086',
    sql="""
    -- Create weather table in DDS (added scd2); at KLAR airport
    CREATE TABLE IF NOT EXISTS dds.h2_weather_klar (
        temperature_key TEXT PRIMARY KEY,
        air_temperature INTEGER,
        pressure_ground DECIMAL(7,2),
        pressure_sea DECIMAL(7,2),
        humidity INTEGER,
        mean_wind TEXT,
        wind_speed INTEGER,
        wind_speed_10_m INTEGER,
        description_rain VARCHAR(100),
        descr_vision VARCHAR(100),
        horiz_vision INTEGER,
        effective_from_dttm TIMESTAMP NOT NULL,
        effective_to_dttm TIMESTAMP DEFAULT '9999-12-31 23:59:59',
        is_current_flg BOOLEAN DEFAULT TRUE,
        is_deleted_flg BOOLEAN DEFAULT FALSE,
        processed_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    dag=dag
)

create_dds_weather_kriw = SQLExecuteQueryOperator(
    task_id='create_dds_weather_kriw',
    conn_id='con_dwh_2024_s086',
    sql="""
    -- Create weather table in DDS (added scd2); at KRIW airport
    CREATE TABLE IF NOT EXISTS dds.h2_weather_kriw (
        temperature_key TEXT PRIMARY KEY,
        air_temperature INTEGER,
        pressure_ground DECIMAL(7,2),
        pressure_sea DECIMAL(7,2),
        humidity INTEGER,
        mean_wind TEXT,
        wind_speed INTEGER,
        wind_speed_10_m INTEGER,
        description_rain VARCHAR(100),
        descr_vision VARCHAR(100),
        horiz_vision INTEGER,
        effective_from_dttm TIMESTAMP NOT NULL,
        effective_to_dttm TIMESTAMP DEFAULT '9999-12-31 23:59:59',
        is_current_flg BOOLEAN DEFAULT TRUE,
        is_deleted_flg BOOLEAN DEFAULT FALSE,
        processed_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    dag=dag
)

load_dds_weather_kjac = SQLExecuteQueryOperator(
    task_id='load_dds_weather_kjac',
    conn_id='con_dwh_2024_s086',
    sql="""
    truncate table dds.h2_weather_kjac;
    with scd2_weather as (
select 
	air_temperature, 
	pressure_ground, 
	pressure_sea, 
	humidity, 
	mean_wind, 
	wind_speed, 
	wind_speed_10_m, 
	description_rain, 
	descr_vision, 
	horiz_vision,
    local_time as effective_from_dttm,
    LEAD(local_time, 1, '5999-01-01'::TIMESTAMP) over (order by local_time asc) - INTERVAL '1 millisecond' as effective_to_dttm,
    true as is_current_flg,
    false as is_deleted_flg,
    now() as processed_dttm
from stg.weather_kjac wk)
insert into dds.h2_weather_kjac (temperature_key, air_temperature, pressure_ground, pressure_sea, humidity, mean_wind, wind_speed, wind_speed_10_m, description_rain, descr_vision, horiz_vision, effective_from_dttm, effective_to_dttm, is_current_flg, is_deleted_flg, processed_dttm)
select
    md5(row(air_temperature, pressure_ground, pressure_sea, humidity, mean_wind, wind_speed, wind_speed_10_m, description_rain, descr_vision, horiz_vision, effective_from_dttm, effective_to_dttm, is_current_flg, is_deleted_flg, processed_dttm)::TEXT) as temperature_key,
	air_temperature, 
	pressure_ground, 
	pressure_sea, 
	humidity, 
	mean_wind, 
	wind_speed, 
	wind_speed_10_m::integer, 
	description_rain, 
	descr_vision, 
	horiz_vision,
    effective_from_dttm,
    effective_to_dttm,
    case when effective_to_dttm > '5998-01-01' then true else false end as is_current_flg,
    is_deleted_flg,
    processed_dttm
from scd2_weather sw
    """,
    dag=dag
)

load_dds_weather_kgcc = SQLExecuteQueryOperator(
    task_id='load_dds_weather_kgcc',
    conn_id='con_dwh_2024_s086',
    sql="""
    truncate table dds.h2_weather_kgcc;
    with scd2_weather as (
select 
	air_temperature, 
	pressure_ground, 
	pressure_sea, 
	humidity, 
	mean_wind, 
	wind_speed, 
	wind_speed_10_m, 
	description_rain, 
	descr_vision, 
	horiz_vision,
    local_time as effective_from_dttm,
    LEAD(local_time, 1, '5999-01-01'::TIMESTAMP) over (order by local_time asc) - INTERVAL '1 millisecond' as effective_to_dttm,
    true as is_current_flg,
    false as is_deleted_flg,
    now() as processed_dttm
from stg.weather_kgcc wk)
insert into dds.h2_weather_kgcc (temperature_key, air_temperature, pressure_ground, pressure_sea, humidity, mean_wind, wind_speed, wind_speed_10_m, description_rain, descr_vision, horiz_vision, effective_from_dttm, effective_to_dttm, is_current_flg, is_deleted_flg, processed_dttm)
select
    md5(row(air_temperature, pressure_ground, pressure_sea, humidity, mean_wind, wind_speed, wind_speed_10_m, description_rain, descr_vision, horiz_vision, effective_from_dttm, effective_to_dttm, is_current_flg, is_deleted_flg, processed_dttm)::TEXT) as temperature_key,
	air_temperature, 
	pressure_ground, 
	pressure_sea, 
	humidity, 
	mean_wind, 
	wind_speed, 
	wind_speed_10_m::integer, 
	description_rain, 
	descr_vision, 
	horiz_vision,
    effective_from_dttm,
    effective_to_dttm,
    case when effective_to_dttm > '5998-01-01' then true else false end as is_current_flg,
    is_deleted_flg,
    processed_dttm
from scd2_weather sw
    """,
    dag=dag
)


load_dds_weather_klar = SQLExecuteQueryOperator(
    task_id='load_dds_weather_klar',
    conn_id='con_dwh_2024_s086',
    sql="""
    truncate table dds.h2_weather_klar;
    with scd2_weather as (
select 
	air_temperature, 
	pressure_ground, 
	pressure_sea, 
	humidity, 
	mean_wind, 
	wind_speed, 
	wind_speed_10_m, 
	description_rain, 
	descr_vision, 
	horiz_vision,
    local_time as effective_from_dttm,
    LEAD(local_time, 1, '5999-01-01'::TIMESTAMP) over (order by local_time asc) - INTERVAL '1 millisecond' as effective_to_dttm,
    true as is_current_flg,
    false as is_deleted_flg,
    now() as processed_dttm
from stg.weather_klar wk)
insert into dds.h2_weather_klar (temperature_key, air_temperature, pressure_ground, pressure_sea, humidity, mean_wind, wind_speed, wind_speed_10_m, description_rain, descr_vision, horiz_vision, effective_from_dttm, effective_to_dttm, is_current_flg, is_deleted_flg, processed_dttm)
select
    md5(row(air_temperature, pressure_ground, pressure_sea, humidity, mean_wind, wind_speed, wind_speed_10_m, description_rain, descr_vision, horiz_vision, effective_from_dttm, effective_to_dttm, is_current_flg, is_deleted_flg, processed_dttm)::TEXT) as temperature_key,
	air_temperature, 
	pressure_ground, 
	pressure_sea, 
	humidity, 
	mean_wind, 
	wind_speed, 
	wind_speed_10_m::integer, 
	description_rain, 
	descr_vision, 
	horiz_vision,
    effective_from_dttm,
    effective_to_dttm,
    case when effective_to_dttm > '5998-01-01' then true else false end as is_current_flg,
    is_deleted_flg,
    processed_dttm
from scd2_weather sw
    """,
    dag=dag
)


load_dds_weather_kriw = SQLExecuteQueryOperator(
    task_id='load_dds_weather_kriw',
    conn_id='con_dwh_2024_s086',
    sql="""
    truncate table dds.h2_weather_kriw;
    with scd2_weather as (
select 
	air_temperature, 
	pressure_ground, 
	pressure_sea, 
	humidity, 
	mean_wind, 
	wind_speed, 
	wind_speed_10_m, 
	description_rain, 
	descr_vision, 
	horiz_vision,
    local_time as effective_from_dttm,
    LEAD(local_time, 1, '5999-01-01'::TIMESTAMP) over (order by local_time asc) - INTERVAL '1 millisecond' as effective_to_dttm,
    true as is_current_flg,
    false as is_deleted_flg,
    now() as processed_dttm
from stg.weather_kriw wk)
insert into dds.h2_weather_kriw (temperature_key, air_temperature, pressure_ground, pressure_sea, humidity, mean_wind, wind_speed, wind_speed_10_m, description_rain, descr_vision, horiz_vision, effective_from_dttm, effective_to_dttm, is_current_flg, is_deleted_flg, processed_dttm)
select
    md5(row(air_temperature, pressure_ground, pressure_sea, humidity, mean_wind, wind_speed, wind_speed_10_m, description_rain, descr_vision, horiz_vision, effective_from_dttm, effective_to_dttm, is_current_flg, is_deleted_flg, processed_dttm)::TEXT) as temperature_key,
	air_temperature, 
	pressure_ground, 
	pressure_sea, 
	humidity, 
	mean_wind, 
	wind_speed, 
	wind_speed_10_m::integer, 
	description_rain, 
	descr_vision, 
	horiz_vision,
    effective_from_dttm,
    effective_to_dttm,
    case when effective_to_dttm > '5998-01-01' then true else false end as is_current_flg,
    is_deleted_flg,
    processed_dttm
from scd2_weather sw
    """,
    dag=dag
)

start >> create_dds_airports >> load_dds_airports >> create_dds_weather_kgcc >> load_dds_weather_kgcc >> create_dds_weather_kjac >> load_dds_weather_kjac >> create_dds_weather_klar >> load_dds_weather_klar >> create_dds_weather_kriw >> load_dds_weather_kriw >> end