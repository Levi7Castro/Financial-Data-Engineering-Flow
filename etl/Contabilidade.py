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
    Lê todos os arquivos CSV do diretório especificado.
    """
    print(f"Lendo arquivos do diretório: {filepath}")
    dados = []

    # CORREÇÃO 1: Usar 'filepath' (argumento da função) e não 'csv_path'
    for arquivo in os.listdir(filepath):
        if arquivo.endswith('.csv'):
            caminho_arquivo = os.path.join(filepath, arquivo)
            print(f" - Lendo: {arquivo}")
            
            # CORREÇÃO 2: Adicionado decimal=',' e thousands='.' para números BR
            df = pd.read_csv(
                caminho_arquivo, 
                sep=";",
                decimal=",",
                thousands="."
            )
            dados.append(df)

    if not dados:
        raise ValueError("Nenhum arquivo CSV encontrado no diretório!")

    # Consolidar os dados
    df = pd.concat(dados, ignore_index=True)
    return df

def transform_data(df):
    """
    Limpeza específica para Contabilidade.
    """
    print("Iniciando transformações de dados...")

    # 1. Renomear colunas para bater com o banco
    # Ajuste os nomes à esquerda conforme estão no seu CSV original
    df = df.rename(columns={
        'DataMovimento': 'Data do Movimento',
        'TipoLancamento': 'Tipo do Lancamento',
        # Garante que StatusLançamento (com ç ou sem) vire o padrão esperado
        'StatusLancamento': 'StatusLançamento' 
    })
    
    # 2. Tratamento de Datas
    if 'Data do Movimento' in df.columns:
        col = 'Data do Movimento'
        df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')
        df[col] = df[col].astype(object).where(df[col].notnull(), None)

    # 3. Tratamento de Valor (Única coluna de valor na Contabilidade)
    if 'Valor' in df.columns:
        # Garante que seja float e substitui NaN por None
        df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce')
        df['Valor'] = df['Valor'].where(pd.notnull(df['Valor']), None)
    
    # CORREÇÃO 3: Removido filtro de "Entrada" e colunas de Juros/Multa
    # que não pertencem a este dataset.

    return df

def load_data(conn, df):
    """
    Carga na tabela fContabilidade.
    """
    cursor = conn.cursor()
    
    try:
        # 1. Limpeza
        print("Limpando tabela fContabilidade...") # Corrigido Log
        cursor.execute("TRUNCATE TABLE public.fContabilidade;")

        # 2. Definição da Ordem das Colunas
        # Certifique-se que o DF tem exatamentes estes nomes após o transform_data
        colunas_ordem = [
            'CodFilial', 'Data do Movimento', 'CodConta', 'CodCentroCusto',
            'StatusLançamento', 'Tipo do Lancamento', 'Lote', 'LoteItem', 'Valor'
        ]

        # Verifica se todas as colunas existem antes de tentar converter
        for col in colunas_ordem:
            if col not in df.columns:
                # Cria a coluna com None se ela faltar no CSV para não quebrar o script
                print(f"Aviso: Coluna '{col}' não encontrada no CSV. Criando vazia.")
                df[col] = None

        # 3. Conversão para Lista
        valores = df[colunas_ordem].to_numpy().tolist()

        # 4. Query SQL
        sql_insert = """
        INSERT INTO public.fContabilidade (
            CodFilial,
            Data_do_Movimento,
            CodConta,
            CodCentroCusto,
            StatusLancamento,
            Tipo_do_Lancamento,
            Lote,
            LoteItem,
            Valor
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (CodFilial, Data_do_Movimento, CodConta, Lote, LoteItem) DO NOTHING;
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

# --- BLOCO PRINCIPAL ---
if __name__ == "__main__":
    # Caminho da pasta onde estão os CSVs
    folder_path = r'C:\Users\livsa\DM_Financeiro\Financial-Data-Engineering-Flow\data\LancamentosContabilidade'
    conexao = None

    try:
        # 1. Extração
        df_raw = extract_data(folder_path)

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