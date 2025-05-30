import datetime
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator


def load_dwh_shell(**kwargs):

    # create stg ods dds wrk schemes
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
    dag_id="dwh_team_21_load_dwh_shell",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    description="cteating stg ods dds wrk schemes",
    tags=["team_21"]
)

LOAD_DWH_SHELL = PythonOperator(
    dag=dag,
    task_id="LOAD_DWH_SHELL",
    python_callable=load_dwh_shell,
    op_kwargs={...},
    provide_context=True
)

LOAD_DWH_SHELL
