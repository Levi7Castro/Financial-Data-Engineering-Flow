import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
import os
import numpy as np

# Configuração Global
load_dotenv(dotenv_path=r"C:\Users\livsa\DM_Financeiro\Financial-Data-Engineering-Flow\config\.env")

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
    df = pd.read_excel(filepath, sheet_name="Empresa")
    return df

def transform_data(df):
    """
    Realiza o tratamento de nulos (NaN -> None) para o SQL.
    """
    print("Iniciando transformações de dados...")
    
    # Converte NaN (do Pandas/Excel) para None (do Python/SQL)
    # Sem isso, o banco trava se houver célula vazia
    df = df.where(pd.notnull(df), None)
    
    return df

def load_data(conn, df):
    """
    Realiza o TRUNCATE e o INSERT na tabela dEmpresa.
    """
    cursor = conn.cursor()
    
    try:
        # 1. Limpeza
        print("Limpando tabela dMascaraDRE...") # Corrigido o texto do log
        cursor.execute("TRUNCATE TABLE public.dEmpresas;")

        # 2. Definição da Ordem das Colunas
        colunas_ordem = ['CodEmpresa', 'RazaoEmpresa', 'Fornecedor', 'Cliente', 'Cidade', 'UF',
       'Pais', 'Estado']
        
        # 3. Conversão para Lista
        valores = df[colunas_ordem].to_numpy().tolist()

        # 4. SQL de INSERT 
        sql_insert = """
        INSERT INTO public.dEmpresas (
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


if __name__ == "__main__":
    excel_path = r"C:\Users\livsa\DM_Financeiro\Financial-Data-Engineering-Flow\data\Cadastros.xlsx"
    conexao = None

    try:
        # 1. Extração
        df_raw = extract_data(excel_path)

        # 2. Transformação (Adicionada de volta)
        df_clean = transform_data(df_raw)

        # 3. Carregamento
        conexao = get_db_connection()
        load_data(conexao, df_clean) # Agora df_clean existe

    except Exception as error:
        print(f"O pipeline falhou: {error}")

    finally:
        if conexao:
            conexao.close()
            print("Conexão encerrada.")