import snowflake.connector
import yaml
import json
import os

# 1. Cargar Configuración y Variables
with open('1__config/config.yaml', 'r') as f:
    config = yaml.safe_load(f)

with open('1__config/vars.json', 'r') as f:
    variables = json.load(f)

# Extraemos el valor "DES" para el reemplazo
env_value = variables.get('environment', 'DES')

# 2. Conexión inicial (Bootstrap)
# Usará DQ82916 desde tu config.yaml
conn = snowflake.connector.connect(
    user=config['user'],
    password=config['password'],
    account=config['account'],
    role='ACCOUNTADMIN'
)

def run_sql_file(file_path, connection, env):
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
        # Reemplazo manual de la variable para que el SQL sea válido
        content = content.replace('{{ environment }}', env)
        
        # Dividimos por punto y coma para ejecutar sentencia por sentencia
        queries = content.split(';')
        for query in queries:
            if query.strip():
                try:
                    connection.cursor().execute(query)
                except Exception as e:
                    print(f"Error en: {file_path} \nQuery: {query[:50]}... \nError: {e}")

# 3. CREAR INFRAESTRUCTURA BÁSICA (Flujo V_1.1.1 a V_1.1.6)
print("--- Iniciando despliegue de Infraestructura ---")
files = [
    '2__infra/migrations/V__1.1.1_roles.sql',
    '2__infra/migrations/V__1.1.2_databases.sql',
    '2__infra/migrations/V__1.1.3_schemas.sql',
    '2__infra/migrations/V__1.1.4_event_table.sql',
    '2__infra/migrations/V__1.1.5_warehouses.sql',
    '2__infra/migrations/V__1.1.6_resource_monitors.sql'
  
]

for file in files:
    print(f"Aplicando: {file}")
    run_sql_file(file, conn, env_value)

conn.close()
print("--- Infraestructura Base Creada ---")

# 4. EJECUTAR SCHEMACHANGE
# Pasamos el password al entorno para que schemachange lo use automáticamente
print("Entregando control a schemachange...")
os.environ['SNOWSQL_PWD'] = str(config['password'])

# Comando optimizado con tus rutas de carpetas
os.system(f'schemachange deploy --config-folder "2__infra" -f "2__infra/migrations" -V "1__config/vars.json" --create-change-history-table')