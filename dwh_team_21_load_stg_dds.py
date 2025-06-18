import datetime
import logging

from airflow import DAG
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

create_dds_airports = SQLExecuteQueryOperator(
    task_id='create_dds_airports',
    conn_id='con_dwh_2024_s086',
    sql="""
    -- Create airports table in DDS (only useful fields)
    DROP TABLE IF EXISTS dds_dict.dict_airports;
    CREATE TABLE dds_dict.dict_airports (
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
        -- fill dict_airports table
        INSERT INTO dds_dict.dict_airports (id, ident, type, name, icao_code, iata_code)
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
    
create_dds_weather = SQLExecuteQueryOperator(
    task_id='create_dds_weather',
    conn_id='con_dwh_2024_s086',
    sql="""
    -- Create weather table in DDS (added scd2);
    DROP TABLE IF EXISTS dds.h2_weather;
    CREATE TABLE dds.h2_weather (
        hash_key TEXT PRIMARY KEY,
        airport_rk TEXT,
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

load_dds_weather = SQLExecuteQueryOperator(
    task_id='load_dds_weather',
    conn_id='con_dwh_2024_s086',
    sql="""
    -- fill h2_weather table
    with scd2_weather_kgcc as (
select 
	'KGCC' as airport_rk,
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
from stg.weather_kgcc wk
), scd2_weather_kjac as (
	select 
	'KJAC' as airport_rk,
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
from stg.weather_kjac wk
), scd2_weather_klar as (
	select 
	'KLAR' as airport_rk,
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
from stg.weather_klar wk
), scd2_weather_kriw as (
	select 
	'KRIW' as airport_rk,
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
from stg.weather_kriw wk
), union_tbls as (
	select * from scd2_weather_kgcc
	union all
	select * from scd2_weather_kjac
	union all
	select * from scd2_weather_klar
	union all
	select * from scd2_weather_kriw
)
insert into dds.h2_weather (hash_key, airport_rk, air_temperature, pressure_ground, pressure_sea, humidity, mean_wind, wind_speed, wind_speed_10_m, description_rain, descr_vision, horiz_vision, effective_from_dttm, effective_to_dttm, is_current_flg, is_deleted_flg, processed_dttm)
select
    md5(row(airport_rk, air_temperature, pressure_ground, pressure_sea, humidity, mean_wind, wind_speed, wind_speed_10_m, description_rain, descr_vision, horiz_vision, effective_from_dttm, effective_to_dttm, is_current_flg, is_deleted_flg, processed_dttm)::TEXT) as hash_key,
	airport_rk,
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
from union_tbls ut
    """,
    dag=dag
)

create_dds_airports >> load_dds_airports >> create_dds_weather >> load_dds_weather