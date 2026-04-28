"""
Deploy Pipeline
===============
Orchestrates the full deployment of the AI Customer Experience Engine on Snowflake.

Pipeline stages:
    1. ``run_migrations`` — DDL infra objects (roles, databases, schemas, warehouses, SPs)
    2. ``run_seed``        — Load PRODUCTS and REVIEWS CSVs into SOURCE layer
    3. ``run_schemachange``— Execute versioned SQL models via schemachange
    4. ``run_docs``        — Generate Sphinx HTML documentation

Usage::

    python R__deploy.py

Connection parameters (account, user, password, environment) are collected
interactively at startup — nothing is hardcoded.
"""

import getpass
import snowflake.connector
from pathlib import Path
import subprocess
import pandas as pd
import math
import os
import json

# =====================================================
# CONFIG  (populated at run time by _prompt_config)
# =====================================================
ACCOUNT      = ""
USER         = ""
PASSWORD     = ""
ENVIRONMENT  = ""
DEPLOY_USER  = ""

WAREHOUSE = ""
DB_SOURCE = ""
DB_ADMIN  = ""
DB_GOLD   = ""
SCHEMA    = "STAGING"
ROLE      = ""

BASE_DIR = Path(__file__).resolve().parent

MIGRATIONS_DIR = BASE_DIR / "2__infra" / "migrations"
SEEDS_DIR      = BASE_DIR / "7__data" / "seeds"

PRODUCTS_FILE = SEEDS_DIR / "PRODUCTS.csv"
REVIEWS_FILE  = SEEDS_DIR / "REVIEWS.csv"


# =====================================================
# INTERACTIVE PROMPT
# =====================================================
def _prompt_config() -> tuple:
    """Collect Snowflake connection parameters from stdin.

    Returns:
        Tuple of ``(account, user, password, environment, deploy_user)``.
    """
    print("=" * 55)
    print("  AI Customer Experience Engine — Deploy")
    print("=" * 55)
    print("  Account identifier format: ORGNAME-ACCOUNTNAME")
    print("  Example: QVHYDSB-JY18582")
    print("-" * 55)
    account     = input("Account identifier  : ").strip()
    user        = input("Login name          : ").strip()
    password    = getpass.getpass("Password            : ")
    environment = input("Environment (DES/PRE/PRO): ").strip().upper()
    deploy_user = input("Deploy user         : ").strip() or user
    print("-" * 55)
    print(f"  Connected as {user} @ {account}  [{environment}]")
    print("=" * 55)
    print()
    return account, user, password, environment, deploy_user


# =====================================================
# CONNECTION
# =====================================================
def get_conn():
    """Return a Snowflake connection as ACCOUNTADMIN (migrations/DDL)."""
    return snowflake.connector.connect(
        user=USER,
        password=PASSWORD,
        account=ACCOUNT,
        role="ACCOUNTADMIN"
    )


def get_project_conn():
    """Return a Snowflake connection as project admin role (seed/data ops)."""
    return snowflake.connector.connect(
        user=USER,
        password=PASSWORD,
        account=ACCOUNT,
        role=ROLE,
        warehouse=WAREHOUSE,
        database=DB_SOURCE,
        schema=SCHEMA
    )


# =====================================================
# SQL SPLITTER
# =====================================================
def split_sql(sql: str):
    """Split a SQL string into individual statements delimited by semicolons.

    Strips blank lines and single-line ``--`` comments before splitting.

    Args:
        sql: Raw SQL text, possibly containing multiple statements.

    Returns:
        List of non-empty SQL statement strings (semicolons removed).
    """
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
    """Execute all SQL statements in a file against the given cursor.

    Replaces ``{{ environment }}`` placeholder with the current ENVIRONMENT
    before execution. Logs each statement result; continues on individual
    failures without raising.

    Args:
        cursor: Active Snowflake cursor.
        file_path: Path to the ``.sql`` file to execute.
    """
    print(f"[INFRA] Applying file: {file_path.name}")

    sql = file_path.read_text(encoding="utf-8")
    sql = sql.replace("{{ environment }}", ENVIRONMENT)
    sql = sql.replace("{{ deploy_user }}", DEPLOY_USER)

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

def clean_value(v):
    """Normalise a single cell value for Snowflake insertion.

    Converts float NaN and common string sentinels (``"nan"``, ``"na"``,
    ``"null"``, ``""``) to ``None`` so Snowflake stores them as ``NULL``.

    Args:
        v: Any scalar value coming from a DataFrame cell.

    Returns:
        The original value, or ``None`` if it represents a missing value.
    """
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    if isinstance(v, str) and v.strip().upper() in ["NAN", "NA", "NULL", ""]:
        return None
    return v


def insert_dataframe(cs, df, table_name, batch_size=100000):
    """Bulk-insert a DataFrame into a Snowflake table using batched executemany.

    Normalises column names to uppercase and converts NaN / empty strings to
    ``None`` so Snowflake stores them as ``NULL``.

    Args:
        cs: Active Snowflake cursor.
        df: DataFrame whose columns must match the target table schema.
        table_name: Fully-qualified or schema-relative table name.
        batch_size: Rows per ``executemany`` call (default 100 000).
    """
    df = df.copy()
    df.columns = [c.upper() for c in df.columns]

    cols = ",".join(df.columns)
    placeholders = ",".join(["%s"] * len(df.columns))

    sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"

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
# LOGGING → TB_LOGS
# =====================================================
_log_cs  = None
_log_tbl = ""


def _log_init() -> None:
    """Create TB_PIPELINE_LOGS (if needed) and open an ACCOUNTADMIN cursor."""
    global _log_cs, _log_tbl
    try:
        conn = snowflake.connector.connect(
            user=USER, password=PASSWORD, account=ACCOUNT, role="ACCOUNTADMIN"
        )
        _log_cs  = conn.cursor()
        _log_tbl = f"DB_ADMIN_{ENVIRONMENT}.LOGS.TB_PIPELINE_LOGS"
        _log_cs.execute(f"""
            CREATE TABLE IF NOT EXISTS {_log_tbl} (
                LOG_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                LEVEL         VARCHAR(10)   DEFAULT 'INFO',
                MESSAGE       VARCHAR(4000)
            )
        """)
        print(f"[LOG] Pipeline logging → {_log_tbl}")
    except Exception as e:
        print(f"[LOG] Warning: pipeline logging unavailable — {e}")
        _log_cs = None


def _log(message: str, level: str = "INFO") -> None:
    """Insert a pipeline event into TB_PIPELINE_LOGS."""
    print(f"[{level}] {message}")
    if _log_cs is None:
        return
    try:
        safe = message.replace("'", "''")
        _log_cs.execute(
            f"INSERT INTO {_log_tbl} (LEVEL, MESSAGE) VALUES ('{level}', '{safe}')"
        )
    except Exception as e:
        print(f"[LOG] Warning: could not write log entry: {e}")


# =====================================================
# INFRA
# =====================================================
def run_migrations():
    """Execute all ``.sql`` migration files in ``2__infra/migrations`` in sorted order."""
    print("INFRA START")
    _log("migration stage started — applying DDL scripts")

    conn = get_conn()
    cs = conn.cursor()

    try:
        for file in sorted(MIGRATIONS_DIR.glob("*.sql")):
            run_sql_file(cs, file)
    finally:
        cs.close()
        conn.close()

    _log("migration stage completed — all DDL scripts applied")


# =====================================================
# SEED
# =====================================================
def run_seed():
    """Load PRODUCTS and REVIEWS seed data into SOURCE tables."""
    print("SEED START")
    _log("seed stage started — loading CSV data into TB_REVIEWS_SRC and TB_PRODUCTS_SRC")

    if not PRODUCTS_FILE.exists():
        raise FileNotFoundError(PRODUCTS_FILE)

    if not REVIEWS_FILE.exists():
        raise FileNotFoundError(REVIEWS_FILE)

    conn = get_project_conn()
    cs = conn.cursor()

    try:
        print("Reading CSV files")

        products = pd.read_csv(PRODUCTS_FILE)
        reviews = pd.read_csv(REVIEWS_FILE)

        print("Loading PRODUCTS")
        insert_dataframe(cs, products, "TB_PRODUCTS_SRC")

        print("Loading REVIEWS")
        insert_dataframe(cs, reviews, "TB_REVIEWS_SRC")

        print("SEED OK")

    finally:
        cs.close()
        conn.close()

    _log("seed stage completed — TB_REVIEWS_SRC and TB_PRODUCTS_SRC loaded")


# =====================================================
# SCHEMACHANGE
# =====================================================

def run_schemachange():
    """Invoke schemachange to deploy versioned SQL models under ``3__models``."""
    print("SCHEMACHANGE START")
    _log("schemachange deploy starting — versioned SQL models")

    env = os.environ.copy()
    env["SNOWFLAKE_ACCOUNT"]   = ACCOUNT
    env["SNOWFLAKE_USER"]      = USER
    env["SNOWFLAKE_PASSWORD"]  = PASSWORD
    env["SNOWFLAKE_ROLE"]      = ROLE
    env["SNOWFLAKE_WAREHOUSE"] = WAREHOUSE
    env["SNOWFLAKE_DATABASE"]  = DB_ADMIN
    env["SNOWFLAKE_SCHEMA"]    = "SCHEMACHANGE"

    vars_payload = json.dumps({"environment": ENVIRONMENT})

    _log("enrichment stage started — cortex AI sentiment scoring and keyword extraction")

    subprocess.run([
        "schemachange",
        "deploy",
        "-f", str(BASE_DIR / "3__models"),
        "-a", ACCOUNT,
        "-u", USER,
        "-r", ROLE,
        "-w", WAREHOUSE,
        "-d", DB_ADMIN,
        "-s", "SCHEMACHANGE",
        "-c", f"{DB_ADMIN}.SCHEMACHANGE.CHANGE_HISTORY",
        "--vars", vars_payload,
        "--schemachange-create-change-history-table"
    ], env=env, check=True)

    _log("enrichment stage completed — cortex sentiment and keyword extraction applied")
    _log("schemachange deploy completed — all models applied")


# =====================================================
# DOCS
# =====================================================
def run_docs():
    """Build Sphinx HTML documentation by delegating to ``R__6.1.1__build_docs.py``."""
    print("DOCS START")

    docs_script = BASE_DIR / "6__scripts" / "R__6.1.1__build_docs.py"

    subprocess.run(
        ["python", str(docs_script)],
        check=True
    )


# =====================================================
# APP DEPLOY (Streamlit in Snowflake)
# =====================================================
def run_app_deploy():
    """PUT app files to STREAMLIT_STAGE and create the Streamlit in Snowflake object."""
    print("APP DEPLOY START")
    _log("app deploy started — uploading streamlit in snowflake files to stage")

    app_dir = BASE_DIR / "4__app"
    stage   = f"@{DB_GOLD}.APPS.STREAMLIT_STAGE"

    conn = get_conn()
    cs   = conn.cursor()

    # Switch to project role so the STREAMLIT object is owned by it, not ACCOUNTADMIN
    cs.execute(f"USE ROLE {ROLE}")
    cs.execute(f"USE WAREHOUSE {WAREHOUSE}")

    def _put(local: Path, stage_path: str) -> None:
        """PUT a local file to the Snowflake stage using FILE:// URI, overwriting existing files."""
        uri = "file://" + str(local.resolve()).replace("\\", "/")
        cs.execute(f"PUT '{uri}' '{stage_path}' OVERWRITE=TRUE AUTO_COMPRESS=FALSE")
        print(f"[APP] PUT {local.name} → {stage_path}")

    try:
        cs.execute(f'DROP STREAMLIT IF EXISTS {DB_GOLD}.APPS.AI_CUSTOMER_EXPERIENCE_ENGINE')
        print("[APP] dropped existing Streamlit (if any)")

        _put(app_dir / "environment.yml", f"{stage}/")
        _put(app_dir / "R__4__app.py",    f"{stage}/")

        for f in sorted((app_dir / "1__pages").glob("*.py")):
            _put(f, f"{stage}/1__pages/")

        for f in sorted((app_dir / "2__services").glob("*.py")):
            _put(f, f"{stage}/2__services/")

        cs.execute(f"""
            CREATE STREAMLIT {DB_GOLD}.APPS.AI_CUSTOMER_EXPERIENCE_ENGINE
            ROOT_LOCATION = '{stage}'
            MAIN_FILE = '/R__4__app.py'
            QUERY_WAREHOUSE = '{WAREHOUSE}'
        """)
        print("APP DEPLOY OK")

    finally:
        cs.close()
        conn.close()

    _log("app deploy completed — streamlit application created successfully")


# =====================================================
# MAIN
# =====================================================
if __name__ == "__main__":
    ACCOUNT, USER, PASSWORD, ENVIRONMENT, DEPLOY_USER = _prompt_config()

    WAREHOUSE = f"WH_ADMIN_{ENVIRONMENT}"
    DB_SOURCE = f"DB_SOURCE_{ENVIRONMENT}"
    DB_ADMIN  = f"DB_ADMIN_{ENVIRONMENT}"
    DB_GOLD   = f"DB_GOLD_{ENVIRONMENT}"
    ROLE      = f"{ENVIRONMENT}_ADMIN_FR"

    run_migrations()   # 1 — DDL: roles, databases, schemas, warehouses, stored procedures
    _log_init()        #     └─ initialise pipeline logging table
    run_seed()         # 2 — Load PRODUCTS.csv and REVIEWS.csv into SOURCE layer
    run_schemachange() # 3 — Deploy versioned SQL models (RAW → SILVER → GOLD → AI)
    run_app_deploy()   # 4 — Upload Streamlit files to stage and recreate SiS app
    run_docs()         # 5 — Build Sphinx HTML documentation
