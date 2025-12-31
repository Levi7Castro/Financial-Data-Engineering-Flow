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
    Lê a Tabela Filial do Excel.
    """
    print(f"Lendo arquivo: {filepath}")
    
    # IMPORTANTE: Mantido header=2 conforme seu código original.
    # Isso significa que o cabeçalho está na linha 3 do Excel.
    df = pd.read_excel(filepath, sheet_name="Filial", header=2) 
    
    return df

def transform_data(df):
    """
    Realiza o tratamento de nulos e renomeia colunas.
    """
    print("Iniciando transformações de dados...")
    
    # Renomeia para bater com o banco de dados
    # Certifique-se que no Excel a coluna se chama 'NomeFilial'
    if 'NomeFilial' in df.columns:
        df = df.rename(columns={'NomeFilial': 'Nome Fantasia'})
    
    # Converte NaN para None (para o PostgreSQL aceitar)
    df = df.where(pd.notnull(df), None)
    
    return df

def load_data(conn, df):
    """
    Realiza o TRUNCATE e o INSERT na tabela dFiliais.
    """
    cursor = conn.cursor()
    
    try:
        # 1. Limpeza
        print("Limpando tabela dFiliais...") 
        cursor.execute("TRUNCATE TABLE public.dFiliais;")

        # 2. Definição da Ordem das Colunas
        colunas_ordem = ['CodFilial', 'Nome Fantasia', 'TipoFilial']
        
        # Validação simples para evitar erro de coluna inexistente
        for col in colunas_ordem:
            if col not in df.columns:
                print(f"AVISO CRÍTICO: Coluna '{col}' não encontrada no DataFrame! O script falhará.")

        # 3. Conversão para Lista
        valores = df[colunas_ordem].to_numpy().tolist()

        # 4. SQL de INSERT 
        sql_insert = """
        INSERT INTO public.dFiliais (
            CodFilial,
            Nome_Fantasia,
            TipoFilial
        ) VALUES (%s, %s, %s)
        ON CONFLICT (CodFilial) DO NOTHING;
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