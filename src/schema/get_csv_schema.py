import os
import pandas as pd
import json

def get_csv_schema(csv_path, output_dir):
    """
    Gera um arquivo JSON contendo o esquema de um arquivo CSV.
    """
    # Ler o arquivo CSV com Pandas
    df = pd.read_csv(csv_path)
    # Inferir o schema
    schema = {col: str(df[col].dtype) for col in df.columns}
    # Salvar o esquema em um arquivo JSON
    os.makedirs(output_dir, exist_ok=True)
    schema_file = os.path.join(output_dir, 'csv_tables.json')
    with open(schema_file, 'w') as f:
        json.dump(schema, f, indent=4)
    print(f"Esquema da tabela em CSV salvo em {schema_file}")

def main():
    csv_path = 'data/raw/order_details.csv'
    output_dir = 'data/metadata'
    get_csv_schema(csv_path, output_dir)

if __name__ == "__main__":
    main()  