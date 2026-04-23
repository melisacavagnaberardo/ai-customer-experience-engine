USE ROLE {{ environment }}_ADMIN_FR;
USE DATABASE DB_ADMIN_{{ environment }};
USE SCHEMA DB_ADMIN_{{ environment }}.PLATFORM;

CREATE OR REPLACE PROCEDURE DB_ADMIN_{{ environment }}.PLATFORM.SP_CREATE_VIEWS(
    "SOURCE_DB" VARCHAR,
    "SOURCE_SCHEMA" VARCHAR,
    "TARGET_DB" VARCHAR,
    "TARGET_SCHEMA" VARCHAR,
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

import traceback

SP_NAME = "SP_CREATE_VIEWS"


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


def view_name(raw_name):
    """Return VW_<name>, stripping any existing VW_ prefix to avoid VW_VW_ doubling."""
    stripped = raw_name.upper()
    if stripped.startswith("VW_"):
        stripped = stripped[3:]
    return f"VW_{stripped}"


def main(session, SOURCE_DB, SOURCE_SCHEMA, TARGET_DB, TARGET_SCHEMA, ENV):

    try:
        session.sql(f"""
            ALTER SESSION SET QUERY_TAG =
            '{SP_NAME}_{ENV}_{SOURCE_SCHEMA}_TO_{TARGET_SCHEMA}'
        """).collect()

        log(session, "INFO", f"START | source={SOURCE_DB}.{SOURCE_SCHEMA} target={TARGET_DB}.{TARGET_SCHEMA}")

        # -----------------------------------------
        # Discover tables and views in source schema
        # -----------------------------------------
        objects = session.sql(f"""
            SELECT TABLE_NAME, TABLE_TYPE
            FROM {SOURCE_DB}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{SOURCE_SCHEMA}'
              AND TABLE_TYPE IN ('BASE TABLE', 'VIEW')
        """).collect()

        created = 0

        # -----------------------------------------
        # Create or replace views in target schema
        # -----------------------------------------
        for obj in objects:
            name     = obj["TABLE_NAME"]
            obj_type = obj["TABLE_TYPE"]

            source_fqn  = f"{SOURCE_DB}.{SOURCE_SCHEMA}.{name}"
            target_view = f"{TARGET_DB}.{TARGET_SCHEMA}.{view_name(name)}"

            try:
                session.sql(f"""
                    CREATE OR REPLACE VIEW {target_view} AS
                    SELECT * FROM {source_fqn}
                """).collect()

                log(session, "INFO", f"VIEW created from {obj_type}: {target_view}")
                created += 1

            except Exception as inner_e:
                log(session, "WARNING", f"SKIP {source_fqn}: {type(inner_e).__name__}: {inner_e}")

        result = f"SUCCESS: {created} views created in {TARGET_DB}.{TARGET_SCHEMA}"
        log(session, "INFO", result)
        log(session, "INFO", "END SUCCESS")

        return result

    except Exception as e:
        try:
            session.sql("ROLLBACK").collect()
        except:
            pass
        log(session, "ERROR", f"EXCEPTION: {type(e).__name__}: {e}")
        log(session, "ERROR", f"TRACEBACK: {traceback.format_exc()}")
        raise

$$;
