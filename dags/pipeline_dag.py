from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import sys
import os

# Add the project root (where config.py lives) to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

# Add project to Python path
sys.path.append("/opt/airflow/pipeline")  # need more explanation on this

from config import pipeline_start
from pipeline.a_bronze.orchestration import source_to_bronze
from utils.freshness_checks import (
    check_movies_file,
    check_ratings_file,
    check_users_file,
)
from pipeline.b_silver.transform import (
    prepare_movie_df,
    prepare_ratings_df,
    prepare_users_df,
)
from pipeline.s_gold.load import *

default_args = {
    "owner": "seyi",
    "email": "marvee006@gmail.com",
    "start_date": pipeline_start,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "sla": timedelta(hours=1),
}

with DAG(
    "Rating Pipeline",
    default_args=default_args,
    description="A DAG to orchestrate ratings data from source to Postgres",
    schedule_interval="@weekly",
    catchup=True,  # enables me to run my pipeline for every missed week from my start_date
) as rating_dag:

    # -------------------
    # Bronze task
    # -------------------
    to_bronze = PythonOperator(task_id="to_bronze", python_callable=source_to_bronze)

    # -------------------
    # Silver tasks
    # -------------------
    check_movie_freshness = ShortCircuitOperator(
        task_id="check_movie_freshness", python_callable=check_movies_file
    )

    check_users_freshness = ShortCircuitOperator(
        task_id="check_users_freshness", python_callable=check_users_file
    )

    check_ratings_freshness = ShortCircuitOperator(
        task_id="check_ratings_freshness", python_callable=check_ratings_file
    )

    movies_to_silver = PythonOperator(
        task_id="load_movies",
        python_callable=prepare_movie_df,
        op_kwargs={"pipeline_start": pipeline_start},
    )

    users_to_silver = PythonOperator(
        task_id="load_users", python_callable=prepare_users_df
    )

    ratings_to_silver = PythonOperator(
        task_id="load_ratings",
        python_callable=prepare_ratings_df,
        op_kwargs={"pipeline_start": pipeline_start},
    )

    # -------------------
    # Gold tasks
    # -------------------
    ensure_staging = PythonOperator(
        task_id="check_create_stg_tables", python_callable=check_tables
    )

    dim_movies_to_gold = PythonOperator(
        task_id="load_movies_to_gold", python_callable=load_movie_df
    )

    dim_users_to_gold = PythonOperator(
        task_id="load_users_to_gold", python_callable=load_users_df
    )

    fct_ratings_to_gold = PythonOperator(
        task_id="load_ratings_to_gold",
        python_callable=load_ratings_df,
        op_kwargs={"pipeline_start": pipeline_start},
    )

    notify_failure = EmailOperator(
        task_id="notify_failure",
        to="marvee006@gmail.com",
        subject="DAG Failed: pipeline_flow",
        html_content="""<h3>One or more tasks in pipeline_flow failed.</h3>""",
        trigger_rule="one_failed",
    )

    notify_success = EmailOperator(
        task_id="notify_success",
        to="oluwaseyiogundimu10@gmail.com",
        subject='DAG Succeeded: pipeline_flow {{ data_interval_end.strftime("%Y-%m-%d") }}',
        html_content="""<h3>All tasks in pipeline_flow completed successfully 🎉</h3>""",
        trigger_rule="all_success",
    )
    # -------------------
    # DAG Dependencies
    # -------------------
    to_bronze >> [check_movie_freshness, check_users_freshness, check_ratings_freshness]

    check_movie_freshness >> movies_to_silver
    check_users_freshness >> users_to_silver
    check_ratings_freshness >> ratings_to_silver

    [movies_to_silver, users_to_silver, ratings_to_silver] >> ensure_staging

    ensure_staging >> [dim_movies_to_gold, dim_users_to_gold] >> fct_ratings_to_gold

    fct_ratings_to_gold >> [notify_failure, notify_success]
