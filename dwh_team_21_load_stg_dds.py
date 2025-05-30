import datetime
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator


def load_stg_dds(**kwargs):

    execution_date = kwargs['execution_date'] + datetime.timedelta(hours=3)

    # STG -> DDS
    ...

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
    start_date=datetime(2025, 1, 1),
    catchup=False,
    description="extraction data from stg to dds",
    tags=["team_21"]
)

LOAD_STG_DDS = PythonOperator(
    dag=dag,
    task_id="LOAD_STG_DDS",
    python_callable=load_stg_dds,
    op_kwargs={...},
    provide_context=True
)

LOAD_STG_DDS
