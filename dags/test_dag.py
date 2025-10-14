# dags/test_email_dag.py
from airflow import DAG
from airflow.operators.email import EmailOperator
from datetime import datetime

with DAG(
    dag_id="test_email_dag",
    start_date=datetime(2025, 10, 1),
    schedule_interval=None,  # only run manually
    catchup=False,
) as dag:

    send_test_email = EmailOperator(
        task_id="send_test_email",
        to="marvee006@gmail.com",
        subject="✅ Airflow Email Test",
        html_content="<h3>This is a test email from Airflow running in Docker.</h3>",
    )