---создадаим календарь 

drop table if exists core.dim_date;
create table core.dim_date
(
  date_dim_pk INT primary key,
  date_actual DATE not null,
  epoch BIGINT not null,
  day_suffix VARCHAR(4) not null,
  day_name VARCHAR(11) not null,
  day_of_week INT not null,
  day_of_month INT not null,
  day_of_quarter INT not null,
  day_of_year INT not null,
  week_of_month INT not null,
  week_of_year INT not null,
  week_of_year_iso CHAR(10) not null,
  month_actual INT not null,
  month_name VARCHAR(9) not null,
  month_name_abbreviated CHAR(3) not null,
  quarter_actual INT not null,
  quarter_name VARCHAR(9) not null,
  year_actual INT not null,
  first_day_of_week DATE not null,
  last_day_of_week DATE not null,
  first_day_of_month DATE not null,
  last_day_of_month DATE not null,
  first_day_of_quarter DATE not null,
  last_day_of_quarter DATE not null,
  first_day_of_year DATE not null,
  last_day_of_year DATE not null,
  mmyyyy CHAR(6) not null,
  mmddyyyy CHAR(10) not null,
  weekend_indr BOOLEAN not null
);

CREATE INDEX dim_date_date_actual_idx
  ON core.dim_date(date_actual);
  
 
 --- заполним данные с помощью процедуры 
 create or replace procedure core.load_date (sdate date,nm integer)
 as $$
 begin 
	 INSERT INTO core.dim_date
	SELECT TO_CHAR(datum, 'yyyymmdd')::INT AS date_dim_id,
	       datum AS date_actual,
	       EXTRACT(EPOCH FROM datum) AS epoch,
	       TO_CHAR(datum, 'fmDDth') AS day_suffix,
	       TO_CHAR(datum, 'TMDay') AS day_name,
	       EXTRACT(ISODOW FROM datum) AS day_of_week,
	       EXTRACT(DAY FROM datum) AS day_of_month,
	       datum - DATE_TRUNC('quarter', datum)::DATE + 1 AS day_of_quarter,
	       EXTRACT(DOY FROM datum) AS day_of_year,
	       TO_CHAR(datum, 'W')::INT AS week_of_month,
	       EXTRACT(WEEK FROM datum) AS week_of_year,
	       EXTRACT(ISOYEAR FROM datum) || TO_CHAR(datum, '"-W"IW-') || EXTRACT(ISODOW FROM datum) AS week_of_year_iso,
	       EXTRACT(MONTH FROM datum) AS month_actual,
	       TO_CHAR(datum, 'TMMonth') AS month_name,
	       TO_CHAR(datum, 'Mon') AS month_name_abbreviated,
	       EXTRACT(QUARTER FROM datum) AS quarter_actual,
	       CASE
	           WHEN EXTRACT(QUARTER FROM datum) = 1 THEN 'First'
	           WHEN EXTRACT(QUARTER FROM datum) = 2 THEN 'Second'
	           WHEN EXTRACT(QUARTER FROM datum) = 3 THEN 'Third'
	           WHEN EXTRACT(QUARTER FROM datum) = 4 THEN 'Fourth'
	           END AS quarter_name,
	       EXTRACT(YEAR FROM datum) AS year_actual,
	       datum + (1 - EXTRACT(ISODOW FROM datum))::INT AS first_day_of_week,
	       datum + (7 - EXTRACT(ISODOW FROM datum))::INT AS last_day_of_week,
	       datum + (1 - EXTRACT(DAY FROM datum))::INT AS first_day_of_month,
	       (DATE_TRUNC('MONTH', datum) + INTERVAL '1 MONTH - 1 day')::DATE AS last_day_of_month,
	       DATE_TRUNC('quarter', datum)::DATE AS first_day_of_quarter,
	       (DATE_TRUNC('quarter', datum) + INTERVAL '3 MONTH - 1 day')::DATE AS last_day_of_quarter,
	       TO_DATE(EXTRACT(YEAR FROM datum) || '-01-01', 'YYYY-MM-DD') AS first_day_of_year,
	       TO_DATE(EXTRACT(YEAR FROM datum) || '-12-31', 'YYYY-MM-DD') AS last_day_of_year,
	       TO_CHAR(datum, 'mmyyyy') AS mmyyyy,
	       TO_CHAR(datum, 'mmddyyyy') AS mmddyyyy,
	       CASE
	           WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN TRUE
	           ELSE FALSE
	           END AS weekend_indr
	FROM (SELECT sdate + SEQUENCE.DAY AS datum
	      FROM GENERATE_SERIES(0, nm - 1) AS SEQUENCE (DAY)
	      order BY SEQUENCE.DAY) DQ
	ORDER BY 1;	
 end;
  
 $$ language plpgsql;
 
-- вызовем процедуру для заполнения 
call core.load_date ('2007-01-01'::date,5843);


drop table if exists core.dim_inventory;
drop table if exists core.dim_date;
drop table if exists core.dim_staff;
drop table if exists core.fact_payment;
drop table if exists core.fact_rental;

-- НАЧАЛЬНЫЙ ВАРИАНТ 
/*create table core.fact_rental (
	rental_pk serial primary key,
	rental_id integer not null,
	inventory_fk integer not null references core.dim_inventory(inventory_pk),
	staff_fk integer not null references core.dim_staff(staff_pk),
	rental_date date not null,
	return_date date,
	cnt int2 not null,
	amount numeric(7,2)
);*/


/*
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

call core.full_load();*/ -- 7 ВАРИАНТ 


-- СВЯЗЫВАЕМ ДАТЫ 
	--добавляем ссылки на rental_date_fk integer not null references core.dim_date(date_dim_pk),
	--return_date_fk integer references core.dim_date(date_dim_pk)
create table core.fact_rental (
	rental_pk serial primary key,
	rental_id integer not null,
	inventory_fk integer not null references core.dim_inventory(inventory_pk),
	staff_fk integer not null references core.dim_staff(staff_pk),
	rental_date_fk integer not null references core.dim_date(date_dim_pk),
	return_date_fk integer references core.dim_date(date_dim_pk),
	cnt int2 not null,
	amount numeric(7,2)
);

---добавляем ссылки на payment_date_fk integer NOT null references core.dim_date(date_dim_pk),
CREATE TABLE core.fact_payment (
	payment_pk serial NOT NULL,
	amount numeric(8, 2) NOT NULL,
	payment_id int4 not null,
	payment_date_fk integer NOT null references core.dim_date(date_dim_pk),
	inventory_fk integer not null references core.dim_inventory(inventory_pk),
	staff_fk integer not null references core.dim_staff(staff_pk),
	PRIMARY KEY (payment_pk)
);


-- заново загрузим данные с учетом каленларя 

create or replace procedure core.load_core_fact_rental()
as $$
begin 
	delete from core.fact_rental;
insert into core.fact_rental	
	(rental_id,
	inventory_fk,
	staff_fk,
	rental_date_fk ,
	return_date_fk ,
	cnt,
	amount)
select 
	sr.rental_id ,
	di.inventory_pk as inventory_fk,
	ds.staff_pk as staff_fk,
	dt_rent.date_dim_pk  as rental_date_fk ,
	dt_return.date_dim_pk  as  return_date_fk ,
	count(*) as cnt,
	sum(p.amount) as amount
from staging.rental sr
join core.dim_staff ds on sr.staff_id = ds.staff_id
join core.dim_inventory di using(inventory_id)
join core.dim_date dt_rent on sr.rental_date::date = dt_rent.date_actual
left join staging.payment p using (rental_id)
left join core.dim_date dt_return on sr.return_date ::date = dt_return.date_actual 
group by 	di.inventory_pk ,
			sr.rental_id ,
			ds.staff_pk ,
			dt_rent.date_dim_pk,
			dt_return.date_dim_pk
;		
end;
$$ language plpgsql;

---------------------------------------------------------

create or replace procedure core.load_core_payment()
as $$
begin 
	
	delete from core.fact_payment;
insert
	into core.fact_payment
	(payment_id,
	amount,
	payment_date_fk,
	inventory_fk,
	staff_fk)
select
	 p.payment_id,
	 p.amount as amount,
	 dt_payment.date_dim_pk  as payment_date_fk,
	 i.inventory_pk as inventory_pk,
	 ds.staff_pk as staff_fk 
from staging.payment p 
join staging.rental r  using (rental_id) 
join core.dim_inventory  i using (inventory_id)
join core.dim_staff ds on p.staff_id = ds.staff_id
join core.dim_date dt_payment on dt_payment.date_actual = p.payment_date::date ;

end;
$$ language plpgsql;
__________________________________________________________________________________________
 --- опять заново загрузим данные 
call core.full_load() 

--- Создадим новый слой для Витрин Данных  -- data mart 

create schema report;

drop table if exists report.sales_date;
create table report.sales_date (
	date_title varchar(20) not null,
	amount numeric(7,2) not null,
	date_sort integer not null
	);


-- создание витрины 
create or replace procedure report.load_sales_date ()
as $$
begin 
	delete from report.sales_date;
	INSERT INTO report.sales_date
			(date_title,--1 сентября 2022 
			amount,
			date_sort)
	select 
		dt.day_of_month ||' '||dt.month_name||' '||dt.year_actual as date_title,
		sum(fp.amount) as amount,
		dt.date_dim_pk  as date_sort
		  
	from core.fact_payment fp 
	join core.dim_date dt on dt.date_dim_pk =fp.payment_date_fk 
	group by 
		dt.day_of_month ||' '||dt.month_name||' '||dt.year_actual,
		dt.date_dim_pk;		
end;

$$ language plpgsql;


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

	call report.load_sales_date ();
end;
$$ language plpgsql;


call core.load_date ('2007-01-01'::date,5843);
call core.full_load();


select * from report.sales_date sd order by date_sort 


-- Создать таблицу и процедуру расчета для отчета, который будет показывать сумму продаж по фильмам.
--Функция, возвращающая данные для визуализации должна возвращать поля:
--film_title - название фильма
--amount - сумму продаж по фильму

drop table if exists report.sales_film;
create table report.sales_film (
	film_title varchar (255 ) not null ,
	amount numeric (7,2) not null 
	) 

create or replace procedure report.load_sales_film()
as $$ 
begin 
	delete from report.sales_film;
	INSERT INTO report.sales_film
				(film_title,
				amount)
	select di.title as film_title, 	
		   sum (fp.amount) as amount
	from core.dim_inventory di 
    join core.fact_payment  fp on fp.inventory_fk  = di.inventory_pk 
	group by 
		di.title;	
end;

$$ language plpgsql;


--Дополнить процедуру full_load() процедурой перерасчета отчета из предыдущего пункта.

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

	call report.load_sales_date ();
	call report.load_sales_film();
end;
$$ language plpgsql;

--- проверка 
call core.full_load()

select * 
from report.sales_film sf 


