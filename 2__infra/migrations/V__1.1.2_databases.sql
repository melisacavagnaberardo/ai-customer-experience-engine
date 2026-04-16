-- 3. Crear DB y Schema para schemachange
USE ROLE {{ environment }}_ADMIN_FR;

CREATE DATABASE IF NOT EXISTS DB_ADMIN_{{ environment }};
CREATE SCHEMA IF NOT EXISTS DB_ADMIN_{{ environment }}.SCHEMACHANGE;