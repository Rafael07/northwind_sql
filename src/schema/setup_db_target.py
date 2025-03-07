import os
import json
import psycopg

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

def read_schema():
    """
    Lê os arquivos de esquema da pasta metadata
    """
    metadata_dir = os.path.join(os.getcwd(), 'metadata')
    schemas = {}
    # Ler schema do banco postgres de origem
    with open(os.path.join(metadata_dir, 'northwind_schema.json'), 'r') as f:
        schemas.update(json.load(f))
    # Ler schema do csv
    with open(os.path.join(metadata_dir, 'csv_tables.json'), 'r') as f:
        schemas.update(json.load(f))
    
    return schemas

def create_tables_if_not_exists(conn, table_name, columns):
    """
    Cria uma tabela no banco de dados se ela ainda não existir.
    """
    with conn.cursor() as cur:
        cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_name = %s   
                    );
                    """, (table_name,))
                    
        table_exists = cur.fetchone()[0]
        
        if not table_exists:
            # Criar a tabela
            columns_def = []
            for column_name, column_type in columns.items():
                #Mapear tipos de dados do pandas/postgres para tipo SQL
                sql_type = map_data_type(column_type)
                columns_def.append(f"{column_name} {sql_type}")

            create_table_sql = f"""
            CREATE TABLE {table_name} (
                {', '.join(columns_def)}
            );
            """
            cur.execute(create_table_sql)
            conn.commit()
            print(f"Tabela {table_name} criada com sucesso.")
        else:
            print(f"Tabela {table_name} já existe.")

def map_data_type(src_type):
    """
    Mapeia tipos de dados do pandas/postgres para tipo SQL.
    """
    # Converter para minúsculo para facilitar a comparação
    src_type = str(src_type).lower()

    type_mapping = {
        'int64': 'INTEGER',
        'float64': 'NUMERIC',
        'object': 'TEXT',
        'datetime64[ns]': 'TIMESTAMP',
        'bool': 'BOOLEAN'
    }

    # Para tipos do Postgres que já estão no formato correto
    if 'character varying' in src_type or 'numeric' in src_type:
        return src_type.upper()
    # Retorna TEXT como tipo padrão
    return type_mapping.get(src_type, 'TEXT')

def main():
    conn = connect_to_target_db()
    try:
        schemas = read_schema()
        for table_name, columns in schemas.items():
            create_tables_if_not_exists(conn, table_name, columns)
        
        print("Setup do banco de dados de destino concluido com sucesso.")

    except Exception as e:
        print(f"Erro ao realizar Setup do banco de dados: {str(e)}")
    finally:
        conn.close

if __name__ == "__main__":
    main()