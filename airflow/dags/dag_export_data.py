from airflow import DAG
from airflow.operators.python import PythonOperator
from extract import *
import pendulum

waktu = pendulum.datetime(2022,12,10, tz="Asia/Jakarta")

with DAG(dag_id = "dag_export_revenue",
    start_date = waktu, 
    schedule_interval = "0 7 * * *",
    catchup = False) as dag:

    revenue = PythonOperator(
        task_id = "export_revenue",
        python_callable = extract_revenue)

    revenue

with DAG(dag_id = "dag_export_song",
    start_date = waktu, 
    schedule_interval = "0 7 * * */3",
    catchup = False) as dag:

    song = PythonOperator(
        task_id = "export_song",
        python_callable = extract_song)

    song

with DAG(dag_id = "dag_export_transaction",
    start_date = waktu, 
    schedule_interval = "0 */3 * * *",
    catchup = False) as dag:

    transactions = PythonOperator(
        task_id = "export_transactions",
        python_callable = extract_transactions)

    transactions