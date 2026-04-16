import snowflake.connector
import yaml
import json
import os

# 1. Cargar Configuración y Variables
with open('1__config/config.yaml', 'r') as f:
    config = yaml.safe_load(f)

with open('1__config/vars.json', 'r') as f:
    variables = json.load(f)

# Extraemos el entorno (ej: 'DES', 'PROD')
env_value = variables.get('environment', 'DES')

# 2. Conexión inicial (Bootstrap con ACCOUNTADMIN)
conn = snowflake.connector.connect(
    user=config['user'],
    password=str(config['password']),
    account=config['account'],
    role='ACCOUNTADMIN'
)

def run_sql_file(file_path, connection, env):
    if not os.path.exists(file_path):
        print(f"Saltando: {file_path} (No encontrado)")
        return
    
    with open(file_path, 'r', encoding='utf-8') as f:
        # Reemplazamos la variable de entorno en el SQL (Bootstrap manual)
        content = f.read().replace('{{ environment }}', env)
        queries = content.split(';')
        for query in queries:
            if query.strip():
                try:
                    connection.cursor().execute(query)
                except Exception as e:
                    print(f"Error en {file_path}: {e}")

# 3. ORDEN LÓGICO DE CREACIÓN (Cimientos de Infraestructura)
print(f"--- Iniciando despliegue de Infraestructura ({env_value}) ---")
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

# 4. EJECUCIÓN DE SCHEMACHANGE (Control de Versiones)
print("--- Entregando control a schemachange ---")

# Preparamos las variables escapadas para que Windows no rompa el JSON
vars_str = json.dumps(variables).replace('"', '\\"')

# Parámetros dinámicos para la conexión de schemachange
snowflake_user = config['user']
snowflake_account = config['account']
snowflake_role = 'ACCOUNTADMIN' 
snowflake_warehouse = f"WH_ADMIN_{env_value}" 
snowflake_database = f"DB_ADMIN_{env_value}"
snowflake_schema = "SCHEMACHANGE"

# Definimos la tabla de historial personalizada para evitar el error de base de datos 'METADATA'
history_table = f"{snowflake_database}.{snowflake_schema}.CHANGE_HISTORY"

# El password se pasa por variable de entorno por seguridad
os.environ['SNOWSQL_PWD'] = str(config['password'])

# Construcción del comando final
command = (
    f'schemachange deploy '
    f'--config-folder "2__infra" '
    f'--root-folder "." '
    f'-a "{snowflake_account}" '
    f'-u "{snowflake_user}" '
    f'-r "{snowflake_role}" '
    f'-w "{snowflake_warehouse}" '
    f'-d "{snowflake_database}" '
    f'-s "{snowflake_schema}" '
    f'-c "{history_table}" '
    f'--vars "{vars_str}" '
    f'--create-change-history-table'
)

print(f"Ejecutando schemachange en {history_table}...")
os.system(command)