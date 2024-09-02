-- Active: 1702571800545@@127.0.0.1@5432@retail_analytics

DROP ROLE IF EXISTS administrator;
DROP ROLE IF EXISTS visitor;

-- Выдача прав для администратора.
CREATE ROLE administrator LOGIN PASSWORD 'password1' SUPERUSER;

-- Выдача прав для посетителя.
CREATE ROLE visitor LOGIN PASSWORD 'password1';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO visitor;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO visitor;

-- Проверка.
SELECT * FROM pg_roles where left(rolname, 2) IN ('ad', 'vi') ;
