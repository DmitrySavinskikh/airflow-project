import datetime
import logging

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook


s3_buck_nm = 'db01-content'
s3_airport_file = 'airports/airports/airports_data.csv'
s3_buck_weather = 'gsbdwhdata'
s3_weather_kgcc = 'weather/dwh_team_21/KGCC.csv'
s3_weather_kjar = 'weather/dwh_team_21/KJAR.csv'
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
        curs = postgres_hook.get_conn().cursor()
        with open(file_name, 'r') as f:
            curs.copy_expert(
                "COPY ods.airports_data FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                f
            )

def load_s3_ods_kgcc():
    s3_hook = S3Hook(aws_conn_id='object_storage_yc')
    file_name = s3_hook.download_file(key=s3_weather_kgcc, bucket_name=s3_buck_weather)

    if file_name:
        postgres_hook = PostgresHook(postgres_conn_id='con_dwh_2024_s086')
        conn = postgres_hook.get_conn()
        curs = conn.cursor()
        with open(file_name, 'r') as f:
            curs.copy_expert(
                "COPY ods.weather_kgcc FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                f
            )
            conn.commit()

def load_s3_ods_kjar():
    s3_hook = S3Hook(aws_conn_id='object_storage_yc')
    file_name = s3_hook.download_file(key=s3_weather_kjar, bucket_name=s3_buck_weather)

    if file_name:
        postgres_hook = PostgresHook(postgres_conn_id='con_dwh_2024_s086')
        conn = postgres_hook.get_conn()
        curs = conn.cursor()
        with open(file_name, 'r') as f:
            curs.copy_expert(
                "COPY ods.weather_kjar FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
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
                "COPY ods.weather_klar FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
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
                "COPY ods.weather_kriw FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                f
            )
            conn.commit()

dag = DAG(
    default_args=dag_default_args,
    dag_id="dwh_team_21_load_s3_ods",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    description="extraction data from s3 to ods",
    tags=["team_21"]
)

start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

airports_CREATE_TBL = PostgresOperator(
    task_id='crt_airports_data_tbl',
    postgres_conn_id='con_dwh_2024_s086',
    sql="""
    CREATE TABLE IF NOT EXISTS ods.airports_data (
        id INTEGER,
        ident VARCHAR(50),
        type VARCHAR(50),
        name VARCHAR(255),
        latitude_deg DECIMAL(10, 6),
        longitude_deg DECIMAL(10, 6),
        elevation_ft INTEGER,
        continent VARCHAR(2),
        iso_country VARCHAR(2),
        iso_region VARCHAR(10),
        municipality VARCHAR(255),
        scheduled_service VARCHAR(3),
        icao_code VARCHAR(4),
        iata_code VARCHAR(3),
        gps_code VARCHAR(10),
        local_code VARCHAR(10),
        home_link VARCHAR(255),
        wikipedia_link VARCHAR(255),
        keywords TEXT
    );
    """,
    dag=dag
)

temperatures_CREATE_TBL = PostgresOperator(
    task_id='crt_temperatures_tbl',
    postgres_conn_id='con_dwh_2024_s086',
    sql="""
    -- Create table for KGCC station
    CREATE TABLE IF NOT EXISTS ods.weather_kgcc (
        local_time TIMESTAMP,
        temperature DECIMAL(4,1),
        pressure_p0 DECIMAL(5,1),
        pressure DECIMAL(5,1),
        humidity INTEGER,
        wind_direction VARCHAR(50),
        wind_speed INTEGER,
        wind_speed_10min INTEGER,
        weather_phenomena VARCHAR(100),
        weather_phenomena_2 VARCHAR(100),
        cloud_coverage VARCHAR(100),
        visibility DECIMAL(4,1),
        dew_point DECIMAL(4,1)
    );

    -- Create table for KJAR station
    CREATE TABLE IF NOT EXISTS ods.weather_kjar (
        local_time TIMESTAMP,
        temperature DECIMAL(4,1),
        pressure_p0 DECIMAL(5,1),
        pressure DECIMAL(5,1),
        humidity INTEGER,
        wind_direction VARCHAR(50),
        wind_speed INTEGER,
        wind_speed_10min INTEGER,
        weather_phenomena VARCHAR(100),
        weather_phenomena_2 VARCHAR(100),
        cloud_coverage VARCHAR(100),
        visibility DECIMAL(4,1),
        dew_point DECIMAL(4,1)
    );

    -- Create table for KLAR station
    CREATE TABLE IF NOT EXISTS ods.weather_klar (
        local_time TIMESTAMP,
        temperature DECIMAL(4,1),
        pressure_p0 DECIMAL(5,1),
        pressure DECIMAL(5,1),
        humidity INTEGER,
        wind_direction VARCHAR(50),
        wind_speed INTEGER,
        wind_speed_10min INTEGER,
        weather_phenomena VARCHAR(100),
        weather_phenomena_2 VARCHAR(100),
        cloud_coverage VARCHAR(100),
        visibility DECIMAL(4,1),
        dew_point DECIMAL(4,1)
    );

    -- Create table for KRIW station
    CREATE TABLE IF NOT EXISTS ods.weather_kriw (
        local_time TIMESTAMP,
        temperature DECIMAL(4,1),
        pressure_p0 DECIMAL(5,1),
        pressure DECIMAL(5,1),
        humidity INTEGER,
        wind_direction VARCHAR(50),
        wind_speed INTEGER,
        wind_speed_10min INTEGER,
        weather_phenomena VARCHAR(100),
        weather_phenomena_2 VARCHAR(100),
        cloud_coverage VARCHAR(100),
        visibility DECIMAL(4,1),
        dew_point DECIMAL(4,1)
    );
    """,
    dag=dag
)

airports_LOAD_S3_ODS = PythonOperator(
    dag=dag,
    task_id=f"load_aiports_data",
    python_callable=load_s3_ods_airports,
    provide_context=True
)

temperatures_kgcc_LOAD_S3_ODS = PythonOperator(
    dag=dag,
    task_id=f"load_temperatures_data_kgcc",
    python_callable=load_s3_ods_kgcc,
    provide_context=True
)

temperatures_kjar_LOAD_S3_ODS = PythonOperator(
    dag=dag,
    task_id=f"load_temperatures_data_kjar",
    python_callable=load_s3_ods_kjar,
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


start >> airports_CREATE_TBL >> airports_LOAD_S3_ODS >> temperatures_CREATE_TBL >> temperatures_kgcc_LOAD_S3_ODS >> temperatures_kjar_LOAD_S3_ODS >> temperatures_klar_LOAD_S3_ODS >> temperatures_kriw_LOAD_S3_ODS >> end
