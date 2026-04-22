USE ROLE {{ environment }}_ADMIN_FR;
USE DATABASE DB_RAW_{{ environment }};
USE SCHEMA DB_RAW_{{ environment }}.RAW;

CREATE OR REPLACE PROCEDURE DB_RAW_{{ environment }}.RAW.SP_UNIVERSAL_BATCH_RAW_INGEST(
    "SOURCE_TABLE" VARCHAR,
    "TARGET_NAME" VARCHAR,
    "ENV" VARCHAR
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS
$$

import hashlib
from snowflake.snowpark.functions import sql_expr

def log(session, level, message):
    try:
        session.sql(f"""
            SELECT SYSTEM$LOG('{level}', '{message}')
        """).collect()
    except:
        pass


def main(session, SOURCE_TABLE, TARGET_NAME, ENV):

    try:
        # -----------------------------------------
        # Session config
        # -----------------------------------------
        session.sql(f"""
            ALTER SESSION SET QUERY_TAG = 'SP_UNIVERSAL_RAW_INGEST_{ENV}'
        """).collect()

        log(session, "INFO", f"START ingestion from {SOURCE_TABLE}")

        target_table = f"DB_RAW_{ENV}.RAW.TB_{TARGET_NAME}"

        # -----------------------------------------
        # Read source
        # -----------------------------------------
        source_df = session.table(SOURCE_TABLE)

        # Add hash — sin cache_result() para no disparar DDL implícito que rompe
        # la transacción scoped antes de tiempo (causa del error 90232)
        source_df = source_df.with_column(
            "ROW_HASH",
            sql_expr("MD5(TO_VARCHAR(ARRAY_CONSTRUCT(*)))")
        )

        schema_cols = source_df.schema.fields

        # -----------------------------------------
        # Create table if not exists (DDL — auto-commit, fuera de transacción)
        # -----------------------------------------
        col_defs = []
        for c in schema_cols:
            if c.name == "ROW_HASH":
                col_defs.append('"ROW_HASH" STRING')
            else:
                col_defs.append(f'"{c.name}" STRING')

        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                {", ".join(col_defs)}
            )
        """

        log(session, "INFO", create_sql)
        session.sql(create_sql).collect()

        log(session, "INFO", f"Table ready: {target_table}")

        # -----------------------------------------
        # Temp view
        # -----------------------------------------
        source_df.create_or_replace_temp_view("STG_SOURCE")

        col_names = [c.name for c in schema_cols]

        insert_cols = ", ".join([f'"{c}"' for c in col_names])
        insert_vals = ", ".join([f's."{c}"' for c in col_names])

        # -----------------------------------------
        # Merge — transacción explícita para evitar error 90232
        # -----------------------------------------
        merge_sql = f"""
        MERGE INTO {target_table} t
        USING STG_SOURCE s
        ON t."ROW_HASH" = s."ROW_HASH"
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols}) VALUES ({insert_vals})
        """

        log(session, "INFO", merge_sql)

        session.sql("BEGIN").collect()
        session.sql(merge_sql).collect()
        session.sql("COMMIT").collect()

        log(session, "INFO", f"MERGE completed into {target_table}")
        log(session, "INFO", "END SUCCESS")

        return f"SUCCESS: {target_table}"

    except Exception as e:
        try:
            session.sql("ROLLBACK").collect()
        except:
            pass
        log(session, "ERROR", str(e))
        raise

$$;
