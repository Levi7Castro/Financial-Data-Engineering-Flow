from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.append("/opt/airflow/etl")  # Adiciona a pasta ETL ao Python path

from Contabilidade import get_db_connection, extract_data, transform_data, load_data

DATA_FOLDER = "/opt/airflow/data/LancamentosContabilidade"

def run_contabilidade_pipeline():
    conexao = None
    try:
        df_raw = extract_data(DATA_FOLDER)
        df_clean = transform_data(df_raw)
        conexao = get_db_connection()
        load_data(conexao, df_clean)
    finally:
        if conexao:
            conexao.close()

with DAG(
    dag_id="update_contabilidade",
    start_date=datetime(2025, 11, 30),
    schedule_interval="@daily",
    catchup=False
) as dag:

    task_run_pipeline = PythonOperator(
        task_id="run_contabilidade_pipeline",
        python_callable=run_contabilidade_pipeline
    )

    task_run_pipeline
