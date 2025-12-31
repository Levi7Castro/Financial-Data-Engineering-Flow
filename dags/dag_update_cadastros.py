from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

sys.path.append("/opt/airflow/etl")

# Empresa
from Empresa import (
    get_db_connection as conn_empresa,
    extract_data as extract_empresa,
    transform_data as transform_empresa,
    load_data as load_empresa
)

# Filial
from Filial import (
    get_db_connection as conn_filial,
    extract_data as extract_filial,
    transform_data as transform_filial,
    load_data as load_filial
)

# Natureza Financeira
from NaturezaFinanceira import (
    get_db_connection as conn_nat,
    extract_data as extract_nat,
    transform_data as transform_nat,
    load_data as load_nat
)

DATA_FILE_PATH = "/opt/airflow/data/Cadastros.xlsx"

# -------------------------
# PIPELINES
# -------------------------
def run_empresa_pipeline():
    conn = None
    try:
        df = extract_empresa(DATA_FILE_PATH)
        df = transform_empresa(df)
        conn = conn_empresa()
        load_empresa(conn, df)
    finally:
        if conn:
            conn.close()

def run_filial_pipeline():
    conn = None
    try:
        df = extract_filial(DATA_FILE_PATH)
        df = transform_filial(df)
        conn = conn_filial()
        load_filial(conn, df)
    finally:
        if conn:
            conn.close()

def run_natureza_pipeline():
    conn = None
    try:
        df = extract_nat(DATA_FILE_PATH)
        df = transform_nat(df)
        conn = conn_nat()
        load_nat(conn, df)
    finally:
        if conn:
            conn.close()

# -------------------------
# DAG
# -------------------------
with DAG(
    dag_id="update_cadastros",
    start_date=datetime(2025, 11, 30),
    schedule_interval="@daily",
    catchup=False,
    tags=["cadastros"]
) as dag:

    t_empresa = PythonOperator(
        task_id="empresa",
        python_callable=run_empresa_pipeline
    )

    t_filial = PythonOperator(
        task_id="filial",
        python_callable=run_filial_pipeline
    )

    t_natureza = PythonOperator(
        task_id="natureza_financeira",
        python_callable=run_natureza_pipeline
    )

    # Ordem (ajuste conforme regra de negócio)
    t_empresa >> t_filial >> t_natureza
