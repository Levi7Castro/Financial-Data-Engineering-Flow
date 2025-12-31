from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

sys.path.append("/opt/airflow/etl")

# MascaraDRE
from MascaraDRE import (
    get_db_connection as mascara_dre,
    extract_data as extract_mascara,
    transform_data as transform_mascara,
    load_data as load_mascara
)

# Plano de contas
from PlanoContas import (
    get_db_connection as conn_PlanoContas,
    extract_data as extract_PlanoContas,
    transform_data as transform_PlanoContas,
    load_data as load_PlanoContas
)


DATA_FILE_PATH = "/opt/airflow/data/PlanoContas.xlsx"

# -------------------------
# PIPELINES
# -------------------------
def run_mascara_pipeline():
    conn = None
    try:
        df = extract_mascara(DATA_FILE_PATH)
        df = transform_mascara(df)
        conn = mascara_dre()
        load_mascara(conn, df)
    finally:
        if conn:
            conn.close()

def run_planocontas_pipeline():
    conn = None
    try:
        df = extract_PlanoContas(DATA_FILE_PATH)
        df = transform_PlanoContas(df)
        conn = conn_PlanoContas()
        load_PlanoContas(conn, df)
    finally:
        if conn:
            conn.close()


# -------------------------
# DAG
# -------------------------
with DAG(
    dag_id="update_planos_contas",
    start_date=datetime(2025, 11, 30),
    schedule_interval="@daily",
    catchup=False,
    tags=["planos_contas"]
) as dag:

    t_mascara = PythonOperator(
        task_id="mascara",
        python_callable=run_mascara_pipeline
    )

    t_planoscontas = PythonOperator(
        task_id="PlanoContas",
        python_callable=run_planocontas_pipeline
    )

    # Ordem (ajuste conforme regra de negócio)
    t_mascara >> t_planoscontas
