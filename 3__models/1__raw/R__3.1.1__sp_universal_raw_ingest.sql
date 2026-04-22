USE ROLE {{ environment }}_ADMIN_FR;
USE DATABASE DB_RAW_{{ environment }};
USE SCHEMA DB_RAW_{{ environment }}.RAW;

CREATE OR REPLACE PROCEDURE DB_RAW_DES.RAW.SP_UNIVERSAL_RAW_INGEST("SOURCE_TABLE" VARCHAR, "TARGET_NAME" VARCHAR, "ENV" VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS $$

import hashlib
from snowflake.snowpark.functions import sql_expr

def log(session, level, message):
    try:
        session.sql(f"""
            SELECT SYSTEM$LOG('{level}', '{message}')
        """).collect()
    except:
        pass

def row_hash(row):
    raw = "|".join([str(v) if v is not None else "" for v in row])
    return hashlib.md5(raw.encode("utf-8")).hexdigest()

def main(session, SOURCE_TABLE, TARGET_NAME, ENV):

    try:

        session.sql(f"""
            ALTER SESSION SET QUERY_TAG = 'SP_UNIVERSAL_RAW_INGEST_{ENV}'
        """).collect()

        log(session, "INFO", f"START ingestion from {SOURCE_TABLE}")

        target_table = f"DB_RAW_{ENV}.RAW.TB_{TARGET_NAME}"

        source_df = session.table(SOURCE_TABLE)

        cols = source_df.columns
        source_df = source_df.with_column(
            "ROW_HASH",
            sql_expr("MD5(TO_VARCHAR(ARRAY_CONSTRUCT(*)))")
        )

        schema_cols = source_df.schema.fields
        col_defs = []

        for c in schema_cols:
            if c.name == "ROW_HASH":
                col_defs.append("ROW_HASH STRING")
            else:
                col_defs.append(f"{c.name} STRING")

        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                {", ".join(col_defs)}
            )
        """

        session.sql(create_sql).collect()
        log(session, "INFO", f"Table ready: {target_table}")

        source_df.create_or_replace_temp_view("STG_SOURCE")

        col_names = [c.name for c in schema_cols]
        insert_cols = ", ".join(col_names)
        insert_vals = ", ".join([f"s.{c}" for c in col_names])

        merge_sql = f"""
        MERGE INTO {target_table} t
        USING STG_SOURCE s
        ON t.ROW_HASH = s.ROW_HASH
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols}) VALUES ({insert_vals})
        """

        session.sql(merge_sql).collect()

        log(session, "INFO", f"MERGE completed into {target_table}")
        log(session, "INFO", "END SUCCESS")

        return f"SUCCESS: {target_table}"

    except Exception as e:
        log(session, "ERROR", str(e))
        raise

$$;