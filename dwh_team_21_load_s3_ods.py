import datetime
import logging

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3Hook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


s3_buck_nm = 'db01-content'
s3_airport_file = 'airports/airports_data.csv'
s3_buck_weather = 'gsbdwhdata'
s3_weather_kgcc = 'weather/dwh_team_21/KGCC.csv'
s3_weather_kjac = 'weather/dwh_team_21/KJAC.csv'
s3_weather_klar = 'weather/dwh_team_21/KLAR.csv'
s3_weather_kriw = 'weather/dwh_team_21/KRIW.csv'


dag_default_args = {
    'owner': 'Savinskikh Dmitry',
    'email': 'dasavinskikh@edu.hse.ru',
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'depends_on_past': False
}

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

def load_s3_ods_kgcc():
    s3_hook = S3Hook(aws_conn_id='object_storage_yc')
    file_name = s3_hook.download_file(key=s3_weather_kgcc, bucket_name=s3_buck_weather)

    if file_name:
        postgres_hook = PostgresHook(postgres_conn_id='con_dwh_2024_s086')
        conn = postgres_hook.get_conn()
        curs = conn.cursor()
        with open(file_name, 'r') as f:
            curs.copy_expert(
                "COPY ods.weather_kgcc FROM STDIN WITH CSV HEADER DELIMITER AS ';' QUOTE '\"'",
                f
            )
            conn.commit()

def load_s3_ods_kjac():
    s3_hook = S3Hook(aws_conn_id='object_storage_yc')
    file_name = s3_hook.download_file(key=s3_weather_kjac, bucket_name=s3_buck_weather)

    if file_name:
        postgres_hook = PostgresHook(postgres_conn_id='con_dwh_2024_s086')
        conn = postgres_hook.get_conn()
        curs = conn.cursor()
        with open(file_name, 'r') as f:
            curs.copy_expert(
                "COPY ods.weather_kjac FROM STDIN WITH CSV HEADER DELIMITER AS ';' QUOTE '\"'",
                f
            )
            conn.commit()
            
def load_s3_ods_klar():
    s3_hook = S3Hook(aws_conn_id='object_storage_yc')
    file_name = s3_hook.download_file(key=s3_weather_klar, bucket_name=s3_buck_weather)

    if file_name:
        postgres_hook = PostgresHook(postgres_conn_id='con_dwh_2024_s086')
        conn = postgres_hook.get_conn()
        curs = conn.cursor()
        with open(file_name, 'r') as f:
            curs.copy_expert(
                "COPY ods.weather_klar FROM STDIN WITH CSV HEADER DELIMITER AS ';' QUOTE '\"'",
                f
            )
            conn.commit()

def load_s3_ods_kriw():
    s3_hook = S3Hook(aws_conn_id='object_storage_yc')
    file_name = s3_hook.download_file(key=s3_weather_kriw, bucket_name=s3_buck_weather)

    if file_name:
        postgres_hook = PostgresHook(postgres_conn_id='con_dwh_2024_s086')
        conn = postgres_hook.get_conn()
        curs = conn.cursor()
        with open(file_name, 'r') as f:
            curs.copy_expert(
                "COPY ods.weather_kriw FROM STDIN WITH CSV HEADER DELIMITER AS ';' QUOTE '\"'",
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

start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

airports_CREATE_TBL = SQLExecuteQueryOperator(
    task_id='crt_airports_data_tbl',
    conn_id='con_dwh_2024_s086',
    sql="""
    CREATE TABLE IF NOT EXISTS ods.airports_data (
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

temperatures_CREATE_TBL = SQLExecuteQueryOperator(
    task_id='crt_temperatures_tbl',
    conn_id='con_dwh_2024_s086',
    sql="""
    -- Create table for KGCC station
    CREATE TABLE IF NOT EXISTS ods.weather_kgcc (
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
    CREATE TABLE IF NOT EXISTS ods.weather_kjac (
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
    CREATE TABLE IF NOT EXISTS ods.weather_klar (
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
    CREATE TABLE IF NOT EXISTS ods.weather_kriw (
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

airports_LOAD_S3_ODS = PythonOperator(
    dag=dag,
    task_id=f"load_airports_data",
    python_callable=load_s3_ods_airports,
    provide_context=True
)

temperatures_kgcc_LOAD_S3_ODS = PythonOperator(
    dag=dag,
    task_id=f"load_temperatures_data_kgcc",
    python_callable=load_s3_ods_kgcc,
    provide_context=True
)

temperatures_kjac_LOAD_S3_ODS = PythonOperator(
    dag=dag,
    task_id=f"load_temperatures_data_kjac",
    python_callable=load_s3_ods_kjac,
    provide_context=True
)

temperatures_klar_LOAD_S3_ODS = PythonOperator(
    dag=dag,
    task_id=f"load_temperatures_data_klar",
    python_callable=load_s3_ods_klar,
    provide_context=True
)

temperatures_kriw_LOAD_S3_ODS = PythonOperator(
    dag=dag,
    task_id=f"load_temperatures_data_kriw",
    python_callable=load_s3_ods_kriw,
    provide_context=True
)


start >> airports_CREATE_TBL >> airports_LOAD_S3_ODS >> temperatures_CREATE_TBL >> temperatures_kgcc_LOAD_S3_ODS >> temperatures_kjac_LOAD_S3_ODS >> temperatures_klar_LOAD_S3_ODS >> temperatures_kriw_LOAD_S3_ODS >> end
