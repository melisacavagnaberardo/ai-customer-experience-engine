USE ROLE {{ environment }}_ADMIN_FR;
USE DATABASE DB_ADMIN_{{ environment }};
USE SCHEMA DB_ADMIN_{{ environment }}.PLATFORM;

CREATE OR REPLACE PROCEDURE DB_ADMIN_{{ environment }}.PLATFORM.SP_UNIVERSAL_BATCH_RAW_INGEST(
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
import traceback
from snowflake.snowpark.functions import sql_expr

SP_NAME = "SP_UNIVERSAL_BATCH_RAW_INGEST"


def log(session, level, message):
    """Write a structured entry to the account event table via SYSTEM$LOG.

    Sanitises single quotes and caps the message at 4 000 characters to avoid
    truncation errors in the event table payload.
    """
    try:
        safe = str(message).replace("'", "''")[:4000]
        session.sql(f"SELECT SYSTEM$LOG('{level}', '[{SP_NAME}] {safe}')").collect()
    except:
        pass


def main(session, SOURCE_TABLE, TARGET_NAME, ENV):
    """Batch-ingest rows from *SOURCE_TABLE* into ``DB_RAW_{ENV}.RAW.TB_{TARGET_NAME}``.

    Strategy:
        1. Reads *SOURCE_TABLE* and computes a ``ROW_HASH`` (MD5 of all columns)
           to enable idempotent merges.
        2. Auto-creates the target table if it does not exist (DDL auto-commits
           to avoid Snowflake error 90232 inside an explicit transaction).
        3. Merges source rows into the target, inserting only new hashes —
           duplicate rows are silently skipped.

    Args:
        session: Active Snowpark session (injected by Snowflake).
        SOURCE_TABLE: Fully-qualified source table name.
        TARGET_NAME: Suffix for the target table (e.g. ``REVIEWS`` → ``TB_REVIEWS``).
        ENV: Environment prefix (DES/PRE/PRO).

    Returns:
        ``"SUCCESS: <target_table>"`` on success; raises on failure after rollback.
    """
    target_table = f"DB_RAW_{ENV}.RAW.TB_{TARGET_NAME}"

    try:
        session.sql(f"ALTER SESSION SET QUERY_TAG = '{SP_NAME}_{ENV}'").collect()

        log(session, "INFO", f"START | source={SOURCE_TABLE} target={target_table}")

        # -----------------------------------------
        # Read source and add row hash
        # -----------------------------------------
        source_df = session.table(SOURCE_TABLE)

        # cache_result() removed — triggers implicit DDL that breaks scoped transactions (error 90232)
        source_df = source_df.with_column(
            "ROW_HASH",
            sql_expr("MD5(TO_VARCHAR(ARRAY_CONSTRUCT(*)))")
        )

        schema_cols = source_df.schema.fields

        # -----------------------------------------
        # DDL — auto-commits, must run before BEGIN
        # -----------------------------------------
        col_defs = [
            '"ROW_HASH" STRING' if c.name == "ROW_HASH" else f'"{c.name}" STRING'
            for c in schema_cols
        ]

        session.sql(f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                {", ".join(col_defs)}
            )
        """).collect()

        log(session, "INFO", f"Table ready: {target_table}")

        # -----------------------------------------
        # Merge — explicit transaction to avoid error 90232
        # -----------------------------------------
        source_df.create_or_replace_temp_view("STG_SOURCE")

        col_names  = [c.name for c in schema_cols]
        insert_cols = ", ".join([f'"{c}"' for c in col_names])
        insert_vals = ", ".join([f's."{c}"' for c in col_names])

        merge_sql = f"""
            MERGE INTO {target_table} t
            USING STG_SOURCE s
            ON t."ROW_HASH" = s."ROW_HASH"
            WHEN NOT MATCHED THEN
                INSERT ({insert_cols}) VALUES ({insert_vals})
        """

        session.sql("BEGIN").collect()
        session.sql(merge_sql).collect()
        session.sql("COMMIT").collect()

        log(session, "INFO", f"MERGE complete | target={target_table}")
        log(session, "INFO", "END SUCCESS")

        return f"SUCCESS: {target_table}"

    except Exception as e:
        try:
            session.sql("ROLLBACK").collect()
        except:
            pass
        log(session, "ERROR", f"EXCEPTION: {type(e).__name__}: {e}")
        log(session, "ERROR", f"TRACEBACK: {traceback.format_exc()}")
        raise

$$;
