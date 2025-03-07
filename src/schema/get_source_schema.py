import json
import os
import psycopg

def connect_to_source_db():
    '''
    Conecta ao banco de dados de origem
    '''
    connection_string = (
        "host='localhost' "
        "dbname='northwind' "
        "user='northwind_user' "
        "password='thewindisblowing' "
        "port=5432"
    )

    conn = psycopg.connect(connection_string)
    return conn

def get_schema(conn, output_dir):
    """
    Gera um arquivo JSON contendo o esquema do banco de dados de origem.
    """
    schema = {}
    with conn.cursor() as cur:
        # Obtém as tabelas do banco no esquema público
        cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public';
        """)
        tables = cur.fetchall()

        for table in tables:
            table_name = table[0]
            # Obter colunas e tipos de dados para cada tabela
            cur.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = '{table_name}';
            """)
            columns = cur.fetchall()
            schema[table_name] = {col[0]: col[1] for col in columns}

    # Salvar o esquema em um arquivo JSON
    os.makedirs(output_dir, exist_ok=True)
    schema_file = os.path.join(output_dir, 'northwind_schema.json')
    with open(schema_file, 'w') as f:
        json.dump(schema, f, indent=4)

    print(f"Esquema do banco Northwind salvo em {schema_file}")

def main():
    conn = connect_to_source_db()
    output_dir = 'data/metadata'
    get_schema(conn, output_dir)
    conn.close()

if __name__ == "__main__":
    main()