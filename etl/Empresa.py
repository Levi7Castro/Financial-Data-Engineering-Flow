import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
import os

# Carrega variáveis de ambiente (Caminho do Airflow)
load_dotenv(dotenv_path="/opt/airflow/config/.env")

def get_db_connection():
    """
    Cria e retorna a conexão com o banco PostgreSQL.
    """
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT")
        )
        return conn
    except Exception as e:
        print(f"Erro ao conectar no banco: {e}")
        raise e
    
def extract_data(filepath):
    """
    Lê a Tabela Empresa do Excel.
    """
    print(f"Lendo arquivo: {filepath}")
    # Lê a aba "Empresa" do Excel
    df = pd.read_excel(filepath, sheet_name="Empresa")
    return df

def transform_data(df):
    """
    Realiza o tratamento de nulos (NaN -> None) para o SQL.
    """
    print("Iniciando transformações de dados...")
    
    # Converte NaN (do Pandas/Excel) para None (do Python/SQL)
    df = df.where(pd.notnull(df), None)
    
    return df

def load_data(conn, df):
    """
    Realiza o TRUNCATE e o INSERT na tabela dEmpresa.
    """
    cursor = conn.cursor()
    
    try:
        # 1. Limpeza
        print("Limpando tabela dEmpresa...") 
        cursor.execute("TRUNCATE TABLE public.dempresas;")

        # 2. Definição da Ordem das Colunas
        colunas_ordem = ['CodEmpresa', 'RazaoEmpresa', 'Fornecedor', 'Cliente', 'Cidade', 'UF', 'Pais', 'Estado']
        
        # 3. Conversão para Lista
        valores = df[colunas_ordem].to_numpy().tolist()

        # 4. SQL de INSERT 
        sql_insert = """
        INSERT INTO public.dempresas (
            CodEmpresa,
            RazaoEmpresa,
            Fornecedor,
            Cliente,
            Cidade,
            UF,
            Pais,
            Estado
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (CodEmpresa) DO NOTHING;
        """

        # 5. Execução
        print(f"Inserindo {len(valores)} registros...")
        execute_batch(cursor, sql_insert, valores, page_size=5000)
        conn.commit()
        print("Carga realizada com sucesso!")

    except Exception as e:
        conn.rollback()
        print(f"Erro durante a carga: {e}")
        raise e
    finally:
        cursor.close()