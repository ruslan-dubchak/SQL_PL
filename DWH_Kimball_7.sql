-- создание staging слоя

-- создание таблиц staging слоя



drop table if exists staging.film;
create table staging.film (
	film_id int not null,
	title varchar(255) not null,
	description text null,
	release_year int2 null,
	language_id int2 not null,
	rental_duration int2 not null,
	rental_rate numeric(4,2) not null,
	length int2 null,
	replacement_cost numeric(5,2) not null,
	rating varchar(10) null,
	last_update timestamp not null,
	special_features _text null,
	fulltext tsvector not null
);



drop table if exists staging.inventory;
create table staging.inventory (
	inventory_id int4 not null,
	film_id int2 not null,
	store_id int2 not null

);



drop table if exists staging.rental;
create table staging.rental (
	rental_id int4 not null,
	rental_date timestamp not null,
	inventory_id int4 not null,
	customer_id int2 not null,
	return_date timestamp null,
	staff_id int2 not null
);



drop table if exists staging.payment;
create table staging.payment (
	payment_id int4 not null,
	customer_id int2 not null,
	staff_id int2 not null,
	rental_id int4 not null,
	amount numeric(5,2) not null,
	payment_date timestamp not null
);



drop table if exists staging.staff;
create table staging.staff (
	staff_id int4 NOT NULL,
	first_name varchar(45) NOT NULL,
	last_name varchar(45) NOT NULL,
	store_id int2 NOT NULL
);



drop table if exists staging.address;
create table staging.address (
	address_id int4 NOT NULL,
	address varchar(50) NOT NULL,
	district varchar(20) NOT NULL,
	city_id int2 NOT NULL
);


drop table if exists staging.city;
CREATE TABLE staging.city (
	city_id int4 NOT NULL,
	city varchar(50) NOT NULL
);



drop table if exists staging.store;
CREATE TABLE staging.store (
	store_id integer NOT NULL,
	address_id int2 NOT NULL

);



-- создание процедур загрузки данных в staging слой



create or replace procedure staging.film_load()
 as $$
begin
delete from staging.film;
insert
into staging.film
	
	(film_id,
	title,
	description,
	release_year,
	language_id,
	rental_duration,
	rental_rate,
	length,
	replacement_cost,
	rating,
	last_update,
	special_features,
	fulltext)
select 
	film_id,
	title,
	description,
	release_year,
	language_id,
	rental_duration,
	rental_rate,
	length,
	replacement_cost,
	rating,
	last_update,
	special_features,
	fulltext
from
	film_src.film;
end;
$$ language plpgsql;



create or replace procedure staging.inventory_load()
as $$
begin
delete from staging.inventory;
insert into staging.inventory
	(
	inventory_id, 
	film_id, 
	store_id
	)
select
	inventory_id, 
	film_id, 
	store_id
from
	film_src.inventory i;
end;
$$ language plpgsql;



create or replace procedure staging.rental_load()
as $$
begin
delete from staging.rental;
insert into staging.rental
	(
	rental_id, 
	rental_date, 
	inventory_id, 
	customer_id, 
	return_date, 
	staff_id
	)
select 
	rental_id, 
	rental_date, 
	inventory_id, 
	customer_id, 
	return_date, 
	staff_id
from
	film_src.rental;
end;
$$ language plpgsql;



create or replace procedure staging.payment_load()
as $$
begin
delete from staging.payment;
insert into staging.payment
(
	payment_id, 
	customer_id, 
	staff_id, 
	rental_id, 
	amount, 
	payment_date
)
select
	payment_id, 
	customer_id, 
	staff_id, 
	rental_id, 
	amount, 
	payment_date
from
	film_src.payment;
end;
$$ language plpgsql;



create or replace procedure staging.staff_load()
as $$
begin 
delete from staging.staff;
insert into staging.staff
	(
	staff_id,
	first_name,
	last_name,
	store_id
	)
select
	staff_id,
	first_name,
	last_name,
	store_id 
from
	film_src.staff s;
end;
$$ language plpgsql;





create or replace procedure staging.address_load()
as $$
begin 
delete from staging.address;
insert into staging.address
	(
	address_id,
	address,
	district,
	city_id
	)
select
	address_id,
	address,
	district,
	city_id
from 
	film_src.address;
end;

$$ language plpgsql;



create or replace procedure staging.city_load()
as $$
begin 
delete from staging.city;
insert into staging.city
	(
	city_id,
	city
	)
select
	city_id,
	city
from
	film_src.city;
end;
$$ language plpgsql;



create or replace procedure staging.store_load()
as $$
begin 
delete from staging.store;
insert into staging.store
	(
	store_id,
	address_id
	)
select
	store_id,
	address_id
from
	film_src.store;
end;
$$ language plpgsql;


--Загрузка данных в Таблицы 

call staging.film_load();
call staging.inventory_load();
call staging.rental_load();
call staging.payment_load();
call staging.staff_load();
call staging.address_load();
call staging.city_load();
call staging.store_load();


select count(*) 
from staging.film f 

-- загрузка core.dim_inventory

create or replace  procedure core.load_core_inventory()
as $$ 
begin 
	
delete from core.dim_inventory;
INSERT INTO core.dim_inventory
	(inventory_id, 
	film_id, title, 
	rental_duration, 
	rental_rate, 
	length, 
	rating)
select 
	i.inventory_id,
	i.film_id,
	f.title,
	f.rental_duration,
	f.rental_rate,
	f.length,
	f.rating
from 
	staging.inventory i
join staging.film f using(film_id);
	
end;
$$ language plpgsql; 

call load_core_inventory()
select count(*) from core.dim_inventory di 


create or replace procedure core.load_core_dim_staff()
as $$
begin 
	delete from core.dim_staff;
insert	into core.dim_staff	
    (staff_id,
	first_name,
	last_name,
	address,
	district,
	city_name)
select 
	s.staff_id ,
	s.first_name ,
	s.last_name,
	a.address,
	a.district,
	c.city 
from 
	staging.staff s 
join staging.store t using (store_id)
join staging.address a using (address_id)
join staging.city c   using (city_id);	
end;

$$ language plpgsql;

call core.load_core_dim_staff();

select * from core.dim_staff ds 

create or replace procedure core.load_core_payment()
as $$
begin 
	
	delete from core.fact_payment;
insert
	into core.fact_payment
	(payment_id,
	amount,
	payment_date,
	inventory_fk,
	staff_fk)
select
	 p.payment_id,
	 p.amount as amount,
	 p.payment_date::date as payment_date,
	 i.inventory_pk as inventory_pk,
	 ds.staff_pk as staff_fk 
from staging.payment p 
join staging.rental r  using (rental_id) 
join core.dim_inventory  i using (inventory_id)
join core.dim_staff ds on p.staff_id = ds.staff_id ;

end;
$$ language plpgsql;

call core.load_core_payment();

select count(*)  from core.fact_payment fp ;

select count(*)  from film_src.payment ;


--Создать процедуру core.load_core_renal, которая будет:
--сначала удалять все из таблицы core.fact_rental
--заполнять таблицу core.fact_rental

create or replace procedure core.load_core_fact_rental()
as $$
begin 
	delete from core.fact_rental;
insert into core.fact_rental	
	(inventory_fk,
	staff_fk,
	rental_date,
	return_date,
	amount)
select 
	di.inventory_pk as inventory_fk,
	ds.staff_pk as staff_fk,
	sr.rental_date::date as rental_date,
	sr.return_date::date as  return_date,
	sum(p.amount) as amount
from staging.rental sr
join core.dim_staff ds on sr.staff_id = ds.staff_id
join core.dim_inventory di using(inventory_id)
left join staging.payment p using (rental_id)
group by 	di.inventory_pk ,
			ds.staff_pk ,
			sr.rental_date::date,
			sr.return_date::date
;		
end;
$$ language plpgsql;

call core.load_core_fact_rental();

select * from core.fact_rental fr ;


-- очистка таблиц Фактов, иначе будет выдавать ошибку при наполнении. 
create or replace  procedure core.delete_fact_table()
as $$
begin
	delete from  core.fact_payment;
	delete from core.fact_rental;	
end;

$$ language plpgsql;

 --Создать sql файл с кодом полного пересоздания таблиц и процедур в staging и core слоях.
-- coздаем процедуру загрузки staging + удаление fact + загрузка core 

create or replace procedure  core.full_load()
as $$
begin 	
	call staging.film_load();
	call staging.inventory_load();
	call staging.rental_load();
	call staging.payment_load();
	call staging.staff_load();
	call staging.address_load();
	call staging.city_load();
	call staging.store_load();
	
	call core.delete_fact_table();
	call core.load_core_inventory();
	call core.load_core_dim_staff();
	call core.load_core_payment();
	call core.load_core_fact_rental();
end;
$$ language plpgsql;

call core.full_load();



--Протестировать полную загрузку данных в хранилище:
--Добавить несколько записей в таблицу rental в источнике
--Запустить процедуру полной выгрузки данных из источника в хранилище --Добавить несколько записей в таблицу rental в источнике
--Проверить, что новая запись из rental попала в core слой хранилища

call public.fill_null_nm_dt(4) -- Это делаем в источнике на другом сервере public 
call core.full_load();

select * from film_src.rental r order by r.last_update desc

select * from core.fact_rental fr order by rental_date desc