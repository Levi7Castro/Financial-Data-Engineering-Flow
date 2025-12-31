from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

sys.path.append("/opt/airflow/etl")

# Pagamentos
from Pagamentos import (
    get_db_connection as conn_pagamentos,
    extract_data as extract_pagamentos,
    transform_data as transform_pagamentos,
    load_data as load_pagamentos
)

# Plano de contas
from Recebimentos import (
    get_db_connection as conn_recebimentos,
    extract_data as extract_recebimentos,
    transform_data as transform_recebimentos,
    load_data as load_recebimentos
)


DATA_FILE_PATH = "/opt/airflow/data/TitulosFinanceiros/LancamentosFinanceiros.csv"

# -------------------------
# PIPELINES
# -------------------------
def run_pagamentos_pipeline():
    conn = None
    try:
        df = extract_pagamentos(DATA_FILE_PATH)
        df = transform_pagamentos(df)
        conn = conn_pagamentos()
        load_pagamentos(conn, df)
    finally:
        if conn:
            conn.close()

def run_recebimentos_pipeline():
    conn = None
    try:
        df = extract_recebimentos(DATA_FILE_PATH)
        df = transform_recebimentos(df)
        conn = conn_recebimentos()
        load_recebimentos(conn, df)
    finally:
        if conn:
            conn.close()


# -------------------------
# DAG
# -------------------------
with DAG(
    dag_id="update_lanc_financeiro",
    start_date=datetime(2025, 11, 30),
    schedule_interval="@daily",
    catchup=False,
    tags=["lancamentos_financeiro"]
) as dag:

    t_pagamentos = PythonOperator(
        task_id="pagamentos",
        python_callable=run_pagamentos_pipeline
    )

    t_recebimentos = PythonOperator(
        task_id="recebimentos",
        python_callable=run_recebimentos_pipeline
    )

    # Ordem (ajuste conforme regra de negócio)
    t_pagamentos >> t_recebimentos
