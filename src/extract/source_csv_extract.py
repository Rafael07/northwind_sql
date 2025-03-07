import pandas as pd
import os
from datetime import datetime 

def read_csv(file_path):
    """
    Lê um arquivo CSV e retorna um DataFrame.
    """
    return pd.read_csv(file_path)

def create_date_dir(base_dir):
    """
    Cria um diretório com o nome da data atual.
    """
    current_date = datetime.now().strftime("%Y-%m-%d")
    date_dir = os.path.join(base_dir, current_date)
    os.makedirs(date_dir, exist_ok=True)
    return date_dir

def save_files_in_date_dir(df, date_dir, filename):
    """
    Salva um DataFrame em um arquivo CSV no diretório da data atual.
    """
    bronze_path = os.path.join(date_dir, filename)
    df.to_csv(bronze_path, index=False)
    print(f"Arquivo {filename} salvo com sucesso no diretório {bronze_path}")

def save_csv_files(raw_dir, bronze_dir):  
    """
    Salva todos os arquivos CSV em um diretório com o nome da data atual.
    """
    date_dir = create_date_dir(bronze_dir)
    for filename in os.listdir(raw_dir):
        if filename.endswith('.csv'):
            csv_path = os.path.join(raw_dir, filename)
            df = pd.read_csv(csv_path)  
            save_files_in_date_dir(df, date_dir, filename)

def main():
    raw_dir = "data/raw" 
    bronze_dir = "data/bronze/csv"
    
    save_csv_files(raw_dir, bronze_dir)

if __name__ == "__main__":
    main()