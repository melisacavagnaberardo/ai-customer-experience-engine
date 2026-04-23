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

def log(session, level, message):
    """
    Logging wrapper usando SYSTEM$LOG para trazabilidad en Snowflake.
    No falla si el logging no está disponible.
    """
    try:
        session.sql(f"SELECT SYSTEM$LOG('{level}', '{message}')").collect()
    except:
        pass


def main(session, SOURCE_DB, SOURCE_SCHEMA, TARGET_DB, TARGET_SCHEMA, ENV):

    try:
        # -------------------------------------------------
        # CONTEXTO DE EJECUCIÓN
        # -------------------------------------------------
        session.sql(f"""
            ALTER SESSION SET QUERY_TAG =
            'SP_MEDALLION_VIEWS_{ENV}_{SOURCE_SCHEMA}_TO_{TARGET_SCHEMA}'
        """).collect()

        log(session, "INFO", f"START scanning {SOURCE_DB}.{SOURCE_SCHEMA}")

        # -------------------------------------------------
        # OBTENER TABLAS Y VISTAS DEL ESQUEMA ORIGEN
        # -------------------------------------------------
        objects = session.sql(f"""
            SELECT TABLE_NAME, TABLE_TYPE
            FROM {SOURCE_DB}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{SOURCE_SCHEMA}'
              AND TABLE_TYPE IN ('BASE TABLE', 'VIEW')
        """).collect()

        created = 0

        # -------------------------------------------------
        # GENERACIÓN DE VISTAS GOLD
        # -------------------------------------------------
        for obj in objects:

            name = obj["TABLE_NAME"]
            obj_type = obj["TABLE_TYPE"]

            source_fqn = f"{SOURCE_DB}.{SOURCE_SCHEMA}.{name}"
            target_view = f"{TARGET_DB}.{TARGET_SCHEMA}.VW_{name}"

            # -------------------------------------------------
            # GOLD SIEMPRE ES VIEW SOBRE EL ORIGEN (tabla o view)
            # -------------------------------------------------
            sql = f"""
            CREATE OR REPLACE VIEW {target_view} AS
            SELECT *
            FROM {source_fqn}
            """

            session.sql(sql).collect()

            log(session, "INFO", f"GOLD VIEW created from {obj_type}: {target_view}")
            created += 1

        # -------------------------------------------------
        # RESULTADO
        # -------------------------------------------------
        result = f"SUCCESS: {created} GOLD views created in {TARGET_DB}.{TARGET_SCHEMA}"

        log(session, "INFO", result)
        log(session, "INFO", "END SUCCESS")

        return result

    except Exception as e:
        log(session, "ERROR", str(e))
        raise

$$;