from datetime import timedelta, datetime
from airflow import DAG

from airflow.operators.python import PythonOperator

from spotify_etl import (extract_user_top_artists, transform_user_top_artists, load_user_top_artists)

default_args = {
    "owner": "Jackson",
    "start_date": datetime(2023, 6, 22),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

etl_dag = DAG(
    "user_top_artists_etl",
    description="ETL process for user top artists data in Spotify",
    default_args=default_args,
    schedule_interval="@daily"
)

extract = PythonOperator(
    task_id="extract_user_top_artists",
    python_callable=extract_user_top_artists,
    dag=etl_dag
)

transform = PythonOperator(
    task_id="transform_user_top_artists",
    python_callable=transform_user_top_artists,
    dag=etl_dag
)

load = PythonOperator(
    task_id="load_user_top_artists",
    python_callable=load_user_top_artists,
    dag=etl_dag
)

extract >> transform >> load
