import snowflake.connector
from pathlib import Path
import subprocess
import pandas as pd
import math
import os
import subprocess
import json

# =====================================================
# CONFIG
# =====================================================
USER = "MCAVAGNA"
PASSWORD = "Larraya2512!.."
ACCOUNT = "PFCBSOA-DQ82916"

WAREHOUSE = "WH_ADMIN_DES"

DB_SOURCE = "DB_SOURCE_DES"
DB_ADMIN = "DB_ADMIN_DES"
SCHEMA = "STAGING"
ENVIRONMENT = "DES"
ROLE = f"{ENVIRONMENT}_ADMIN_FR"

BASE_DIR = Path(__file__).resolve().parent

MIGRATIONS_DIR = BASE_DIR / "2__infra" / "migrations"
SEEDS_DIR = BASE_DIR / "9__data" / "seeds"

PRODUCTS_FILE = SEEDS_DIR / "PRODUCTS.csv"
REVIEWS_FILE = SEEDS_DIR / "REVIEWS.csv"


# =====================================================
# CONNECTION
# =====================================================
def get_conn():
    return snowflake.connector.connect(
        user=USER,
        password=PASSWORD,
        account=ACCOUNT,
        warehouse=WAREHOUSE,
        role="ACCOUNTADMIN"
    )


# =====================================================
# SQL SPLITTER
# =====================================================
def split_sql(sql: str):
    statements = []
    buffer = []

    for line in sql.splitlines():
        line_clean = line.strip()

        if not line_clean or line_clean.startswith("--"):
            continue

        buffer.append(line)

        if line_clean.endswith(";"):
            statements.append("\n".join(buffer).strip().rstrip(";"))
            buffer = []

    if buffer:
        statements.append("\n".join(buffer).strip())

    return [s for s in statements if s]


# =====================================================
# SQL EXECUTOR
# =====================================================
def run_sql_file(cursor, file_path: Path):
    print(f"[INFRA] Applying file: {file_path.name}")

    sql = file_path.read_text(encoding="utf-8")
    sql = sql.replace("{{ environment }}", "DES")

    statements = split_sql(sql)

    executed = 0
    failed = 0

    for stmt in statements:
        try:
            cursor.execute(stmt)
            executed += 1
        except Exception as e:
            failed += 1
            print(f"[ERROR] SQL failed in {file_path.name}")
            print(e)

    print(f"[INFRA] Completed {file_path.name} | executed={executed} failed={failed}")


# =====================================================
# INSERT FUNCTION 
# =====================================================

def insert_dataframe(cs, df, table_name, batch_size=100000):
    df = df.copy()
    df.columns = [c.upper() for c in df.columns]

    cols = ",".join(df.columns)
    placeholders = ",".join(["%s"] * len(df.columns))

    sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"

    def clean_value(v):
        if v is None:
            return None
        if isinstance(v, float) and math.isnan(v):
            return None
        if isinstance(v, str) and v.strip().upper() in ["NAN", "NA", "NULL", ""]:
            return None
        return v

    data = [
        tuple(clean_value(v) for v in row)
        for row in df.to_numpy()
    ]

    total = len(data)
    print(f"[LOAD] {table_name} rows={total} batch_size={batch_size}")

    for i in range(0, total, batch_size):
        batch = data[i:i + batch_size]
        cs.executemany(sql, batch)

        print(f"[LOAD] inserted {min(i+batch_size, total)}/{total}")

# =====================================================
# INFRA
# =====================================================
def run_migrations():
    print("INFRA START")

    conn = get_conn()
    cs = conn.cursor()

    try:
        for file in sorted(MIGRATIONS_DIR.glob("*.sql")):
            run_sql_file(cs, file)

    finally:
        cs.close()
        conn.close()


# =====================================================
# SEED
# =====================================================
def run_seed():
    print("SEED START")

    if not PRODUCTS_FILE.exists():
        raise FileNotFoundError(PRODUCTS_FILE)

    if not REVIEWS_FILE.exists():
        raise FileNotFoundError(REVIEWS_FILE)

    conn = get_conn()
    cs = conn.cursor()

    try:
        cs.execute(f"USE WAREHOUSE {WAREHOUSE}")
        cs.execute(f"USE DATABASE {DB_SOURCE}")
        cs.execute(f"USE SCHEMA {SCHEMA}")

        print("Reading CSV files")

        products = pd.read_csv(PRODUCTS_FILE)
        reviews = pd.read_csv(REVIEWS_FILE)

        print("Truncating tables")

        cs.execute("TRUNCATE TABLE TB_PRODUCTS_SRC")
        cs.execute("TRUNCATE TABLE TB_REVIEWS_SRC")

        print("Loading PRODUCTS")
        insert_dataframe(cs, products, "TB_PRODUCTS_SRC")

        print("Loading REVIEWS")
        insert_dataframe(cs, reviews, "TB_REVIEWS_SRC")

        print("SEED OK")

    finally:
        cs.close()
        conn.close()


# =====================================================
# SCHEMACHANGE
# =====================================================

def run_schemachange():
    print("SCHEMACHANGE START")

    # Definir variables de entorno correctamente
    env = os.environ.copy()
    env["SNOWFLAKE_ACCOUNT"] = ACCOUNT
    env["SNOWFLAKE_USER"] = USER
    env["SNOWFLAKE_PASSWORD"] = PASSWORD
    env["SNOWFLAKE_ROLE"] = "DES_ADMIN_FR"
    env["SNOWFLAKE_WAREHOUSE"] = WAREHOUSE
    env["SNOWFLAKE_DATABASE"] = DB_ADMIN
    env["SNOWFLAKE_SCHEMA"] = "SCHEMACHANGE"

    vars_payload = json.dumps({
        "environment": "DES"
    })

    subprocess.run([
        "schemachange",
        "deploy",
        "-f", str(BASE_DIR / "3__models"),
        "-a", ACCOUNT,
        "-u", USER,
        "-r", "DES_ADMIN_FR",
        "-w", WAREHOUSE,
        "-d", DB_ADMIN,
        "-s", "SCHEMACHANGE",
        "-c", f"{DB_ADMIN}.SCHEMACHANGE.CHANGE_HISTORY",
        "--vars", vars_payload,
        "--schemachange-create-change-history-table"
    ], env=env, check=True)


# =====================================================
# MAIN
# =====================================================
if __name__ == "__main__":
    run_migrations()
    run_seed()
    run_schemachange()
