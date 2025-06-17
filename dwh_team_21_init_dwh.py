from datetime import datetime
from airflow import DAG
from airflow.decorators import dag  # Добавьте этот импорт
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

@dag(
    dag_id="dwh_team_21_init_dwh",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    description="Инициализация DWH: создание схем ods, stg, dds",
    tags=["team_21"]
)
def init_dwh_dag():

    def check_connection():
        """Проверка подключения к PostgreSQL с использованием PostgresHook"""
        hook = PostgresHook(postgres_conn_id="con_dwh_2024_s086")
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        conn.close()
        if result[0] != 1:
            raise ValueError("Connection test failed")
        print("Connection successful!")

    # Проверка подключения
    check_connection_task = PythonOperator(
        task_id='check_connection',
        python_callable=check_connection
    )

    # Создание схем
    create_schemas = SQLExecuteQueryOperator(
        task_id='create_schemas',
        sql=[
            "CREATE SCHEMA IF NOT EXISTS ods;",
            "CREATE SCHEMA IF NOT EXISTS stg;",
            "CREATE SCHEMA IF NOT EXISTS dds;",
            "CREATE SCHEMA IF NOT EXISTS dds_dict;",
            "CREATE SCHEMA IF NOT EXISTS dm;"
        ],
        autocommit=True,
        conn_id="con_dwh_2024_s086",
        split_statements=True
    )

    check_connection_task >> create_schemas

dwh_instance = init_dwh_dag()