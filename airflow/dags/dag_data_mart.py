from airflow import DAG
from airflow.operators.python import PythonOperator
from data_mart import *
import pendulum

waktu = pendulum.datetime(2022,12,11, tz="Asia/Jakarta")

with DAG(dag_id = "dag_data_mart",
    start_date = waktu, 
    schedule_interval = "0 9 * * *",
    catchup = False) as dag:

    penjualan = PythonOperator(
        task_id = "pendapatan_penjualan",
        python_callable = pendapatan_penjualan)

    lokasi = PythonOperator(
        task_id = "lokasi_customer",
        python_callable = lokasi_customer)
    
    pendapatan = PythonOperator(
        task_id = "pendapatan_artist",
        python_callable = pendapatan_artist)

    produktif = PythonOperator(
        task_id = "artist_produktif",
        python_callable = artist_produktif)

    penjualan
    lokasi
    pendapatan
    produktif