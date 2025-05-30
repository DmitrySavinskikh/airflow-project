import datetime
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator


def load_src_ods(**kwargs):

    execution_date = kwargs['execution_date'] + datetime.timedelta(hours=3)

    # SRC -> ODS
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
    dag_id="dwh_team_21_load_s3_ods",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    description="extraction data from s3 to ods",
    tags=["team_21"]
)

LOAD_S3_ODS = PythonOperator(
    dag=dag,
    task_id="LOAD_S3_ODS",
    python_callable=load_src_ods,
    op_kwargs={...},
    provide_context=True
)

LOAD_S3_ODS
