
--создание полклбчения к другой ДБ ( в нашем случае посгрес паблик )
--Проверим подключения 
select * from pg_catalog.pg_available_extensions;

-- установим расширение для подключения
create extension postgres_fdw;

--Созаем сервер и оболочку 
create server film_pg foreign data wrapper postgres_fdw options (
	host 'localhost',
	dbname 'postgres',
	port '5432'
	);

--Создаем пользователя для мутирования 
create user mapping for postgres server film_pg options (
	user 'postgres',
	password 'admin'
	);	
--создание схемы поключения 
drop schema if exists film_src;
create schema film_src authorization postgres;

-- перенос типов данных 
--DROP TYPE if exists "mpaa_rating";
CREATE TYPE public."mpaa_rating" AS ENUM (
	'G',
	'PG',
	'PG-13',
	'R',
	'NC-17');
	
-- DROP DOMAIN public."year";
CREATE DOMAIN public.year AS integer
CHECK (VALUE >= 1901 AND VALUE <= 2155);

--перенос данных - связь с film_src
import foreign schema public from server film_pg into film_src;

-- создание Схем 
create schema staging;
create schema ods;
create schema ref;

