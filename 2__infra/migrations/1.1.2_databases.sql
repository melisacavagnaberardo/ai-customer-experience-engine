-- 3. Crear DB y Schema para schemachange
USE ROLE {{ environment }}_ADMIN_FR;

CREATE DATABASE IF NOT EXISTS DB_ADMIN_{{ environment }};
CREATE SCHEMA IF NOT EXISTS DB_ADMIN_{{ environment }}.SCHEMACHANGE;

CREATE DATABASE IF NOT EXISTS DB_ADMIN_{{ environment }};

-- RAW
CREATE DATABASE IF NOT EXISTS DB_RAW_{{ environment }};

-- SILVER
CREATE DATABASE IF NOT EXISTS DB_SILVER_{{ environment }};

-- GOLD
CREATE DATABASE IF NOT EXISTS DB_GOLD_{{ environment }};

