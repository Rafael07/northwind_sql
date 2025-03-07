import os
import pandas as pd
from datetime import datetime
import psycopg

def connect_to_postgres():
    """
    Conecta ao banco de dados PostgreSQL.
    """
    conn = psycopg.connect(
            host="localhost",
            dbname="northwind",
            user="northwind_user",
            password="thewindisblowing",
            port=5432
        )
    return conn

def create_dir(base_dir, table_name):
    """
    Cria um diretório na camada Bronze com o nome da tabela e da data atual.
    """
    current_date = datetime.now().strftime("%Y-%m-%d")
    postgres_dir = os.path.join(base_dir, table_name, current_date)
    os.makedirs(postgres_dir, exist_ok=True)
    return postgres_dir

def extract_data_from_postgres(conn, table_name, bronze_dir):
    """
    Extrai dados do banco de dados PostgreSQL e salva em arquivos CSV.
    """
    query = f"SELECT * FROM {table_name}"
    with conn.cursor() as cur:
        cur.execute(query)
        df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
    csv_file = f"{table_name}.csv"
    csv_file_path = os.path.join(bronze_dir, csv_file)
    df.to_csv(csv_file_path, index=False)
    print(f"Arquivo {csv_file} salvo com sucesso no diretório {bronze_dir}")

def get_table_names(conn):
    """
    Obtem os nomes das tabelas do banco de dados PostgreSQL.
    """
    query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
    """
    with conn.cursor() as cur:
        cur.execute(query)
        table_names = [row[0] for row in cur.fetchall()]
    return table_names

def main():
    base_dir = "data/bronze/postgres"

    conn = connect_to_postgres()
    table_names = get_table_names(conn)

    for table_name in table_names:
        postgres_dir = create_dir(base_dir, table_name)
        extract_data_from_postgres(conn, table_name, postgres_dir)
    
    conn.close()

if __name__ == "__main__":
    main()