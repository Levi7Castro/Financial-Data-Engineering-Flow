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
    Lê a Tabela Plano de Contas do Excel.
    """
    print(f"Lendo arquivo: {filepath}")
    # CORREÇÃO: Usar o 'filepath' passado por parâmetro, não fixo.
    df = pd.read_excel(filepath, sheet_name="PlanoContas")
    return df

def transform_data(df):
    """
    Realiza o tratamento de nulos.
    """
    print("Iniciando transformações de dados...")
    
    # 1. Tratamento de Nulos
    colunas_nivel = ['Nivel1DRE', 'Nivel2DRE']
    # Fillna garante que não tenhamos NaN
    df[colunas_nivel] = df[colunas_nivel].fillna("NÃO SE APLICA")
    
    return df

def load_data(conn, df):
    """
    Realiza o TRUNCATE e o INSERT na tabela dPlanoDeContas.
    """
    cursor = conn.cursor()
    
    try:
        # 1. Limpeza (Target: dPlanoDeContas)
        print("Limpando tabela dPlanoDeContas...")
        cursor.execute("TRUNCATE TABLE public.dPlanoDeContas;")

        # 2. Definição da Ordem das Colunas
        colunas_ordem = ['CodGrupoDRE', 'CodContaContabil', 'ContaContabil', 'Classificacao', 'Nivel1DRE', 'Nivel2DRE']
        
        # 3. Conversão para Lista
        valores = df[colunas_ordem].to_numpy().tolist()

        # 4. Query SQL (CORRIGIDO: Apontando para dPlanoDeContas com 6 placeholders)
        sql_insert = """
        INSERT INTO public.dPlanoDeContas (
            CodGrupoDRE, 
            CodContaContabil, 
            ContaContabil, 
            Classificacao,
            Nivel1DRE, 
            Nivel2DRE
        ) VALUES (%s, %s, %s, %s, %s, %s);
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

# --- BLOCO PRINCIPAL (ORQUESTRADOR) ---
if __name__ == "__main__":
    # CORREÇÃO: O caminho aqui deve ser do Excel, não do CSV anterior
    excel_path = r"C:\Users\livsa\DM_Financeiro\Financial-Data-Engineering-Flow\data\PlanoContas.xlsx"
    conexao = None

    try:
        # 1. Extração
        df_raw = extract_data(excel_path)

        # 2. Transformação
        df_clean = transform_data(df_raw)

        # 3. Carregamento
        conexao = get_db_connection()
        load_data(conexao, df_clean)

    except Exception as error:
        print(f"O pipeline falhou: {error}")

    finally:
        if conexao:
            conexao.close()
            print("Conexão encerrada.")