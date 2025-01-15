import psycopg2
import json

# Load metadata from the JSON file
with open('metadata.json', 'r') as f:
    metadata = json.load(f)['metadata']

# Database connection
DATABASE = metadata['database_info']

conn = psycopg2.connect(
    host=DATABASE['host'],
    dbname=DATABASE['dbname'],  
    user=DATABASE['user'],    
    password= DATABASE['password'] 
)
cur = conn.cursor()


def create_columns_sql(columns):
    column_defs = []
    for column in columns:
        column_def = f"{column['name']} {column['type']}"
        if column.get('primary_key', False):
            column_def += " PRIMARY KEY"
        if column.get('nullable', False) is False:
            column_def += " NOT NULL"
        if 'foreign_key' in column:
            column_def += f" REFERENCES {column['foreign_key']}"
        column_defs.append(column_def)
    return ", ".join(column_defs)


def create_table_sql(table):
    table_name = table['name']
    columns_sql = create_columns_sql(table['columns'])
    return f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_sql});"


fact_table_sql = create_table_sql(metadata['fact_table'])
# cur.execute(fact_table_sql)
print("Generated Fact Table", fact_table_sql)


for dim_table in metadata['dimension_tables']:
    dim_table_sql = create_table_sql(dim_table)
    # cur.execute(dim_table_sql)
    print("Generated Dimension table", dim_table_sql)


conn.commit()
cur.close()
conn.close()

print("Tables created successfully.")

