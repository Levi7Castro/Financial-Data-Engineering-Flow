import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
import os
import numpy as np

# Configuração Global
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
    
    # CORREÇÃO IMPORTANTE: Adicionei header=2 aqui se sua planilha 
    # realmente tiver linhas vazias no topo antes do cabeçalho.
    # Se o cabeçalho for na primeira linha, remova o parameter 'header=2'.
    df = pd.read_excel(filepath, sheet_name="Filial", header=2) 
    
    return df

def transform_data(df):
    """
    Realiza o tratamento de nulos.
    """
    print("Iniciando transformações de dados...")
    
    # Converte NaN para None
    df = df.where(pd.notnull(df), None)
    df = df.rename(columns={'NomeFilial': 'Nome Fantasia'})
    
    return df

def load_data(conn, df):
    """
    Realiza o TRUNCATE e o INSERT na tabela dFilial.
    """
    cursor = conn.cursor()
    
    try:
        # 1. Limpeza
        print("Limpando tabela dFilial...") 
        # Padronizado para Singular (dFilial) para evitar erro de tabela inexistente
        cursor.execute("TRUNCATE TABLE public.dFiliais;")

        # 2. Definição da Ordem das Colunas
        # Certifique-se que no Excel o nome é exatamente 'Nome Fantasia'
        colunas_ordem = ['CodFilial', 'Nome Fantasia', 'TipoFilial']
        
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

# --- BLOCO PRINCIPAL (CORRIGIDO) ---
if __name__ == "__main__":
    # 1. Definição correta do caminho (apenas STRING)
    excel_path = r"C:\Users\livsa\DM_Financeiro\Financial-Data-Engineering-Flow\data\Cadastros.xlsx"
    
    conexao = None

    try:
        # 2. Extração (Passando o caminho, não o dataframe)
        df_raw = extract_data(excel_path)

        # 3. Transformação
        df_clean = transform_data(df_raw)

        # 4. Carregamento
        conexao = get_db_connection()
        load_data(conexao, df_clean)

    except Exception as error:
        print(f"O pipeline falhou: {error}")

    finally:
        if conexao:
            conexao.close()
            print("Conexão encerrada.")