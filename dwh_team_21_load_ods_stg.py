import datetime
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator


def load_ods_stg(**kwargs):

    execution_date = kwargs['execution_date'] + datetime.timedelta(hours=3)

    # ODS -> STG
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
    dag_id="dwh_team_21_load_ods_stg",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    description="extraction data from ods to stg",
    tags=["team_21"]
)

LOAD_ODS_STG = PythonOperator(
    dag=dag,
    task_id="LOAD_ODS_STG",
    python_callable=load_ods_stg,
    op_kwargs={...},
    provide_context=True
)

LOAD_ODS_STG
