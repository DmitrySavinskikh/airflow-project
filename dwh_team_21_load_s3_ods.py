import datetime
import logging

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3Hook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


# specify .csv-file locations in s3
s3_buck_nm = 'db01-content'
s3_airport_file = 'airports/airports_data.csv'
s3_buck_weather = 'gsbdwhdata'
s3_weather_kgcc = 'weather/dwh_team_21/KGCC.csv'
s3_weather_kjac = 'weather/dwh_team_21/KJAC.csv'
s3_weather_klar = 'weather/dwh_team_21/KLAR.csv'
s3_weather_kriw = 'weather/dwh_team_21/KRIW.csv'


dag_default_args = {
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'depends_on_past': False
}

# load into ods.airports_data table
def load_s3_ods_airports():
    s3_hook = S3Hook(aws_conn_id='object_storage_yc')
    file_name = s3_hook.download_file(key=s3_airport_file, bucket_name=s3_buck_nm)

    if file_name:
        postgres_hook = PostgresHook(postgres_conn_id='con_dwh_2024_s086')
        conn = postgres_hook.get_conn()
        curs = conn.cursor()
        with open(file_name, 'r') as f:
            curs.copy_expert(
                "COPY ods.airports_data FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                f
            )
            conn.commit()

# load into ods.weather_<airport_icao> tables
def load_s3_ods_weather():
    s3_hook = S3Hook(aws_conn_id='object_storage_yc')
    weather_dict = {
        'weather_kgcc': s3_weather_kgcc,
        'weather_kjac': s3_weather_kjac,
        'weather_klar': s3_weather_klar,
        'weather_kriw': s3_weather_kriw
    }

    for pair in weather_dict.items():
        file_name = s3_hook.download_file(key=pair[1], bucket_name=s3_buck_weather)

        if file_name:
            postgres_hook = PostgresHook(postgres_conn_id='con_dwh_2024_s086')
            conn = postgres_hook.get_conn()
            curs = conn.cursor()
            with open(file_name, 'r') as f:
                curs.copy_expert(
                    f"COPY ods.{pair[0]} FROM STDIN WITH CSV HEADER DELIMITER AS ';' QUOTE '\"'",
                    f
                )
                conn.commit()

dag = DAG(
    default_args=dag_default_args,
    dag_id="dwh_team_21_load_s3_ods",
    schedule=None,
    start_date=datetime.datetime(2025, 1, 1),
    catchup=False,
    description="extraction data from s3 to ods",
    tags=["team_21"]
)

# task for creating airport table
airports_CREATE_TBL = SQLExecuteQueryOperator(
    task_id='crt_airports_data_tbl',
    conn_id='con_dwh_2024_s086',
    sql="""
    DROP TABLE IF EXISTS ods.airports_data;
    CREATE TABLE ods.airports_data (
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
    """,
    dag=dag
)

# task for creating weather tables
weather_CREATE_TBL = SQLExecuteQueryOperator(
    task_id='crt_weather_tbl',
    conn_id='con_dwh_2024_s086',
    sql="""
    -- Create table for KGCC station
    DROP TABLE IF EXISTS ods.weather_kgcc;
    CREATE TABLE ods.weather_kgcc (
        local_time text default null,
        T decimal(4,1) default null,
        P0 decimal(5,1) default null,
        P decimal(5,1) default null,
        U integer default null,
        DD text default null,
        Ff integer default null,
        ff10 text default null,
        WW text default null,
        W_W_ text default null,
        c text default null,
        VV decimal(4,1) default null,
        Td decimal(4,1) default null,
        col14 text default null
    );

    -- Create table for KJAC station
    DROP TABLE IF EXISTS ods.weather_kjac;
    CREATE TABLE ods.weather_kjac (
        local_time text default null,
        T decimal(4,1) default null,
        P0 decimal(5,1) default null,
        P decimal(5,1) default null,
        U integer default null,
        DD text default null,
        Ff integer default null,
        ff10 text default null,
        WW text default null,
        W_W_ text default null,
        c text default null,
        VV decimal(4,1) default null,
        Td decimal(4,1) default null,
        col14 text default null
    );

    -- Create table for KLAR station
    DROP TABLE IF EXISTS ods.weather_klar;
    CREATE TABLE ods.weather_klar (
        local_time text default null,
        T decimal(4,1) default null,
        P0 decimal(5,1) default null,
        P decimal(5,1) default null,
        U integer default null,
        DD text default null,
        Ff integer default null,
        ff10 text default null,
        WW text default null,
        W_W_ text default null,
        c text default null,
        VV decimal(4,1) default null,
        Td decimal(4,1) default null,
        col14 text default null
    );

    -- Create table for KRIW station
    DROP TABLE IF EXISTS ods.weather_kriw;
    CREATE TABLE ods.weather_kriw (
        local_time text default null,
        T decimal(4,1) default null,
        P0 decimal(5,1) default null,
        P decimal(5,1) default null,
        U integer default null,
        DD text default null,
        Ff integer default null,
        ff10 text default null,
        WW text default null,
        W_W_ text default null,
        c text default null,
        VV decimal(4,1) default null,
        Td decimal(4,1) default null,
        col14 text default null
    );
    """,
    dag=dag
)

# task for load into airports table
airports_LOAD_S3_ODS = PythonOperator(
    dag=dag,
    task_id=f"load_airports_data",
    python_callable=load_s3_ods_airports,
    provide_context=True
)

# task for load into weather tables
weather_LOAD_S3_ODS = PythonOperator(
    dag=dag,
    task_id=f"load_weather",
    python_callable=load_s3_ods_weather,
    provide_context=True
)


airports_CREATE_TBL >> airports_LOAD_S3_ODS >> weather_CREATE_TBL >> weather_LOAD_S3_ODS
