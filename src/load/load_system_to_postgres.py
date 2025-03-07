import os
import pandas as pd
import psycopg
from datetime import datetime

def connect_to_target_db():
    '''
    Conecta ao banco de dados de destino
    '''
    connection_string = (
        "host='localhost' "
        "dbname='northwind_target' "
        "user='target_user' "
        "password='thewindkeepsblowing' "
        "port=5433"
    )

    conn = psycopg.connect(connection_string)
    return conn

def is_date_formated(string):
    """
    Verifica se uma string está formatada como uma data no estilo 'YYYY-MM-DD'
    """
    try:
        datetime.strptime(string, '%Y-%m-%d')
        return True
    except ValueError:
        return False

def get_dates_from_dir(base_dir):
    """
    Retorna um conjunto de datas válidas encontradas nos sbudiretórios
    """
    dates = set()
    for root, dirs, files in os.walk(base_dir):
        for dir in dirs:
            if is_date_formated(dir):
                dates.add(dir)
    return dates

def get_latest_directory(base_dir_csv, base_dir_postgres):
    '''
    Obtem o caminho do diretório mais recente no formato 'YYYY-MM-DD'
    '''
    dates_csv = get_dates_from_dir(base_dir_csv)
    dates_postgres = get_dates_from_dir(base_dir_postgres)
    common_dates = dates_csv.intersection(dates_postgres)
    latest_date = max(common_dates)
    return latest_date
  
def load_data_to_target(conn, csv_path, table_name):
    '''
    Carrega os dados de um arquivo CSV para o banco de dados de destino
    '''
    df = pd.read_csv(csv_path)
    with conn.cursor() as cur:
        for index, row in df.iterrows():
            columns=', '.join(row.index)
            values = ', '.join([f"%s" for _ in row])
            insert_query = f'INSERT INTO {table_name} ({columns}) VALUES ({values})'
            cur.execute(insert_query, tuple(row))   
    conn.commit()
    print(f"Arquivo {csv_path} carregado com sucesso para a tabela {table_name}")

def process_data(conn, base_dir, date):
    target_dir = os.path.join(base_dir, date)
    for root, dirs, files in os.walk(target_dir):
        for filename in files:
            if filename.endswith('.csv'):
                csv_path = os.path.join(root, filename)
                table_name = os.path.splitext(filename)[0]
                load_data_to_target(conn, csv_path, table_name)

def main():
    base_dir_csv = 'data/bronze/csv'
    base_dir_postgres = 'data/bronze/postgres'
    
    conn = connect_to_target_db()

    latest_common_date = get_latest_directory(base_dir_csv, base_dir_postgres)    
    if latest_common_date:
        process_data(conn, base_dir_csv, latest_common_date)
        process_data(conn, base_dir_postgres, latest_common_date)
    else:
        print("Nenhum diretório com data comum encontrado nos diretórios.")

    conn.close()

if __name__ == "__main__":
    main()