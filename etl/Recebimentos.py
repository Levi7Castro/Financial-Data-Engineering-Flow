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
    Cria e retorna a conexão com o banco PostgreSQL usando variáveis de ambiente.
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
    Lê o arquivo CSV com configurações otimizadas para números brasileiros.
    """
    print(f"Lendo arquivo: {filepath}")
    df = pd.read_csv(
        filepath, 
        sep=";", 
        decimal=",",    # Converte vírgula para ponto
        thousands="."   # Remove ponto de milhar
    )
    return df

def transform_data(df):
    """
    Realiza a limpeza de datas e tratamento de nulos (NaN -> None).
    """
    print("Iniciando transformações de dados...")
    
    # 1. Tratamento de Datas
    cols_data = ['Data de Emissao', 'Data de Vencimento', 'Data de Pagamento']
    for col in cols_data:
        # dayfirst=True corrige o aviso e garante leitura DD/MM/AAAA
        df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')
        # Garante conversão de NaT para None
        df[col] = df[col].astype(object).where(df[col].notnull(), None)

    # 2. Tratamento de Valores Numéricos (Garantir None em vez de NaN)
    cols_valores = ["Valor Original", "Valor Juros", "Valor Multa", "Valor Desconto", "Valor Pago"]
    for col in cols_valores:
        df[col] = df[col].where(pd.notnull(df[col]), None)

    df = df[df["Tipo Movimento"] == "Entrada"]

    colunas_excluir = ["Origem", "Tipo Movimento"]
    df = df.drop(columns=colunas_excluir)
        
    return df

def load_data(conn, df):
    """
    Realiza o TRUNCATE e o INSERT em lote no PostgreSQL.
    """
    cursor = conn.cursor()
    
    try:
        # 1. Limpeza
        print("Limpando tabela fPagamentos...")
        cursor.execute("TRUNCATE TABLE public.fRecebimentos;")

        # 2. Definição da Ordem das Colunas (Para bater com o INSERT)
        colunas_ordem = [
            "Data de Emissao", "Data de Vencimento", "Data de Pagamento",
            "CodFilial", "CodEmpresa", "CodTipoDespesa", "CodContaContabil",
            "CodBanco", "CodCategoria", "Status",
            "CodMovimento", "Parcela", "Programacao",
            "Valor Original", "Valor Juros", "Valor Multa", "Valor Desconto", "Valor Pago"
        ]

        # 3. Conversão para Lista de Tuplas (Alta Performance)
        valores = df[colunas_ordem].to_numpy().tolist()

        # 4. Query SQL
        sql_insert = """
        INSERT INTO public.fRecebimentos (
            Data_de_Emissao, Data_de_Vencimento, Data_de_Pagamento,
            CodFilial, CodEmpresa, CodTipoDespesa, CodContaContabil,
            CodBanco, CodCategoria, Status,
            CodMovimento, Parcela, Programacao,
            Valor_Original, Valor_Juros, Valor_Multa, Valor_Desconto, Valor_Pago
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
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
    csv_path = r'C:\Users\livsa\Downloads\BaseDados-20251130T002854Z-1-001\BaseDados\TitulosFinanceiros\LancamentosFinanceiros.csv'
    conexao = None

    try:
        # 1. Extração
        df_raw = extract_data(csv_path)

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