-- 3. Crear DB y Schema para schemachange
USE ROLE {{ environment }}_ADMIN_FR;
USE DATABASE DB_ADMIN_{{ environment }};
CREATE SCHEMA IF NOT EXISTS DB_ADMIN_{{ environment }}.SCHEMACHANGE;
CREATE SCHEMA IF NOT EXISTS DB_ADMIN_{{ environment }}.LOGS;


CREATE SCHEMA IF NOT EXISTS DB_SILVER_{{ environment }}.SILVER;
CREATE SCHEMA IF NOT EXISTS DB_GOLD_{{ environment }}.GOLD;
CREATE SCHEMA IF NOT EXISTS DB_RAW_{{ environment }}.RAW;