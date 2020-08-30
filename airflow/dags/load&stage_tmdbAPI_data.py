from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'luleosi',
    'depends_on_past':False,
    'start_date': datetime(2020, 8, 28),
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
    'email_on_retry': False,
    'catchup_by_default': False,
    'schedule_interval': '@daily'
}

dag = DAG('load_stage_tmdbAPIdata_dag',
          default_args=default_args,
          description='Loads JSON data from the TMDB API and stores it in an S3 bucket '
        )

start_operator = DummyOperator(task_id='Start_DAG_execution', dag=dag)

# DAG Dependencies
