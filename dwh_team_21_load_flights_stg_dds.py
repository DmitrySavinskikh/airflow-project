from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import TaskInstance
import logging

logger = logging.getLogger(__name__)

DAG_ID = 'dwh_team_21_flights_dds'
DEPENDENT_DAG_ID = 'dwh_team_21_load_stg_dds'
PG_CONN_ID = 'con_dwh_2024_s086'
TEAM_AIRPORTS = ('JAC', 'LAR', 'GCC', 'RIW')
MAX_RETRIES = 3

@dag(
    dag_id=DAG_ID,
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2024, 1, 1),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval=None,
    catchup=False,
    tags=['team_21'],
)
def flights_to_dds_dag():

    @task(retries=MAX_RETRIES, retry_delay=timedelta(minutes=2))
    def verify_dependencies():
        """Проверяет наличие всех необходимых таблиц и данных"""
        hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
        try:
            with hook.get_conn() as conn:
                with conn.cursor() as cur:
                    # Проверка существования таблицы аэропортов
                    cur.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_schema = 'dds_dict' 
                        AND table_name = 'dict_airports'
                    );
                    """)
                    table_exists = cur.fetchone()[0]
                    
                    if not table_exists:
                        logger.warning("Таблица dds_dict.dict_airports не найдена")
                        raise ValueError("Таблица аэропортов не найдена")
                    
                    # Проверка наличия нужных аэропортов
                    cur.execute("""
                    SELECT iata_code FROM dds_dict.dict_airports
                    WHERE iata_code IN %s;
                    """, (tuple(TEAM_AIRPORTS),))
                    
                    found_airports = {row[0] for row in cur.fetchall()}
                    missing_airports = set(TEAM_AIRPORTS) - found_airports
                    
                    if missing_airports:
                        logger.warning(f"Отсутствуют данные для аэропортов: {missing_airports}")
                        raise ValueError(f"Отсутствуют данные для аэропортов: {missing_airports}")
                    
                    return True
                    
        except Exception as e:
            logger.error(f"Ошибка при проверке зависимостей: {str(e)}")
            raise

    trigger_dependent_dag = TriggerDagRunOperator(
        task_id='trigger_dependent_dag',
        trigger_dag_id=DEPENDENT_DAG_ID,
        wait_for_completion=True,
        poke_interval=60,
        reset_dag_run=True
    )

    @task
    def create_dds_schema():
        sql = """        
        CREATE TABLE IF NOT EXISTS dds.completed_flights (
            flight_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            carrier_flight_num VARCHAR(10),
            flight_date DATE,
            scheduled_dep_tm TIMESTAMPTZ,
            actual_dep_tm TIMESTAMPTZ,
            origin_airport_dk VARCHAR(10),
            dest_airport_dk VARCHAR(10),
            carrier_code VARCHAR(5),
            tail_num VARCHAR(10),
            distance_miles INTEGER,
            dep_delay_min INTEGER,
            arr_delay_min INTEGER,
            carrier_delay_min INTEGER,
            weather_delay_min INTEGER,
            nas_delay_min INTEGER,
            security_delay_min INTEGER,
            late_aircraft_min INTEGER,
            wheels_off_tm TIMESTAMPTZ,
            wheels_on_tm TIMESTAMPTZ,
            processed_dttm TIMESTAMPTZ DEFAULT NOW(),
            CONSTRAINT uniq_flight UNIQUE (carrier_flight_num, scheduled_dep_tm, origin_airport_dk)
        );
        
        CREATE TABLE IF NOT EXISTS dds.cancelled_flights (
            cancellation_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            carrier_flight_num VARCHAR(10),
            scheduled_dep_tm TIMESTAMPTZ,
            origin_airport_dk VARCHAR(10),
            dest_airport_dk VARCHAR(10),
            carrier_code VARCHAR(5),
            cancellation_code CHAR(1),
            cancellation_reason TEXT GENERATED ALWAYS AS (
                CASE cancellation_code
                    WHEN 'A' THEN 'Carrier'
                    WHEN 'B' THEN 'Weather'
                    WHEN 'C' THEN 'National Air System'
                    WHEN 'D' THEN 'Security'
                    ELSE 'Unknown'
                END
            ) STORED,
            processed_dttm TIMESTAMPTZ DEFAULT NOW(),
            CONSTRAINT uniq_cancellation UNIQUE (carrier_flight_num, scheduled_dep_tm, origin_airport_dk, origin_airport_dk)
        );
        """
        hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
        hook.run(sql)

    @task
    def load_completed_flights():
        sql = """
        INSERT INTO dds.completed_flights
        select
            ...
        from stg.flights
        """
        hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
        hook.run(sql, parameters=(tuple(TEAM_AIRPORTS),))

    @task
    def load_cancelled_flights():
        sql = """
        INSERT INTO dds.cancelled_flights (...)
        -- Ваш SQL запрос здесь
        """
        hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
        hook.run(sql, parameters=(tuple(TEAM_AIRPORTS),))

    # Определение workflow
    verify_task = verify_dependencies()
    trigger_task = trigger_dependent_dag
    
    # Если verify_dependencies завершается с ошибкой, запускаем trigger_dependent_dag
    verify_task >> trigger_task
    trigger_task >> verify_dependencies.retry()
    
    # При успешной проверке продолжаем выполнение
    verify_task >> create_dds_schema() >> [load_completed_flights(), load_cancelled_flights()]

flights_dag = flights_to_dds_dag()