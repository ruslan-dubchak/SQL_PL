--- Создание и наполнение Staging слоя----
 ---- last_update ----
drop table if exists staging.last_update;
create table staging.last_update (
	table_name varchar(50) not null,
	update_dt timestamp not null
);

-----Film-------
drop table if exists staging.film;
CREATE TABLE staging.film (
	film_id int2 NOT NULL,
	title varchar(255) NOT NULL,
	description text NULL,
	release_year public."year" NULL,
	language_id int2 NOT NULL,
	rental_duration int2 NOT NULL,
	rental_rate numeric(4, 2) NOT NULL,
	length int2 NULL,
	replacement_cost numeric(5, 2) NOT NULL,
	rating public."mpaa_rating" NULL ,
	last_update timestamp NOT NULL,
	special_features _text NULL,
	fulltext tsvector NOT NULL
);

---------------Inventory-----------------
drop table if exists staging.inventory;
CREATE TABLE staging.inventory (
	inventory_id int4 NOT NULL,
	film_id int2 NOT NULL,
	store_id int2 NOT NULL,
	last_update timestamp NOT NULL ,
	deleted timestamp NULL
);

-----------Rental-------------------------
drop table if exists staging.rental;
CREATE TABLE staging.rental (
	rental_id int4 NOT NULL,
	rental_date timestamp NOT NULL,
	inventory_id int4 NOT NULL,
	customer_id int2 NOT NULL,
	return_date timestamp NULL,
	staff_id int2 NOT NULL,
	last_update timestamp NOT NULL,
	deleted timestamp NULL
);


---------------Address-------------- 
drop table if exists staging.address;
CREATE TABLE staging.address (
	address_id int4 NOT NULL,
	address varchar(50) NOT NULL,
	address2 varchar(50) NULL,
	district varchar(20) NOT NULL,
	city_id int2 NOT NULL,
	postal_code varchar(10) NULL,
	phone varchar(20) NOT NULL,
	last_update timestamp NOT NULL 
);

---------------City---------------
drop table if exists staging.city;
CREATE TABLE staging.city (
	city_id int2 NOT NULL,
	city varchar(50) NOT NULL,
	country_id int2 NOT NULL,
	last_update timestamp NOT NULL 
);

---------------Staff---------------
drop table if exists staging.staff;
CREATE TABLE staging.staff (
	staff_id int2 NOT NULL,
	first_name varchar(45) NOT NULL,
	last_name varchar(45) NOT NULL,
	address_id int2 NOT NULL,
	email varchar(50) NULL,
	store_id int2 NOT NULL,
	active bool NOT NULL ,
	username varchar(16) NOT NULL,
	"password" varchar(40) NULL,
	last_update timestamp NOT NULL ,
	picture bytea NULL,
	deleted timestamp NULL
);

-------------Store-----------------
DROP TABLE if exists staging.store;
CREATE TABLE staging.store (
	store_id int2 NOT NULL,
	manager_staff_id int2 NOT NULL,
	address_id int2 NOT NULL,
	last_update timestamp NOT NULL 
);

----------------Payment---------------

DROP TABLE if exists staging.payment;
CREATE TABLE staging.payment (
	payment_id int4 NOT NULL,
	customer_id int2 NOT NULL,
	staff_id int2 NOT NULL,
	rental_id int4 NOT NULL,
	amount numeric(5, 2) NOT NULL,
	payment_date timestamp NOT NULL,
	last_update timestamp NOT NULL,
	deleted timestamp NULL
);

--------------------------------------------------------------------------------------------------------------------------------------------------
-- создание процедур загрузки данных в staging слой
------------------------------------------------------------------------------------------------------------------------------------------------
create or replace function staging.get_last_update_table(table_name varchar) returns timestamp
as $$
	begin
		return coalesce( 
			(
				select
					max(update_dt)
				from
					staging.last_update lu
				where 
					lu.table_name = get_last_update_table.table_name
			),
			'1900-01-01'::date	
		);
	end;
$$ language plpgsql;


--- Установка времени ----------------------

create or replace procedure staging.set_table_load_time(table_name varchar, current_update_dt timestamp default now())
as $$
	begin
		INSERT INTO staging.last_update
		(
			table_name, 
			update_dt
		)
		VALUES(
			table_name, 
			current_update_dt
		);
	end;
$$ language plpgsql;

---------------------Film_load in Staging---------------------------------
create or replace procedure staging.film_load(current_update_dt timestamp)
 as $$
	begin
		delete from staging.film;

		insert
		into staging.film
			(
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
			)
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
		
		call staging.set_table_load_time('staging.film', current_update_dt);
	end;
$$ language plpgsql;


--------------------Inventory_load in Staging---------------------------------
create or replace procedure staging.inventory_load(current_update_dt timestamp)
as $$
	begin	
		delete from staging.inventory;

		insert into staging.inventory
		(
			inventory_id, 
			film_id, 
			store_id,
			last_update,
			deleted			
		)
		select 
			inventory_id, 
			film_id, 
			store_id,
			last_update,
			deleted
		from
			film_src.inventory i;
		
		call staging.set_table_load_time('staging.inventory', current_update_dt);
	end;
$$ language plpgsql;


---------------------Rental_load in Staging---------------------------------
create or replace procedure staging.rental_load(current_update_dt timestamp)
as $$
	declare 
		last_update_dt timestamp;
	begin
		last_update_dt = staging.get_last_update_table('staging.rental');
	
		delete from staging.rental;

		insert into staging.rental
		(
			rental_id, 
			rental_date, 
			inventory_id, 
			customer_id, 
			return_date, 
			staff_id,
			last_update,
			deleted
		)
		select 
			rental_id, 
			rental_date, 
			inventory_id, 
			customer_id, 
			return_date, 
			staff_id,
			last_update,
			deleted
		from
			film_src.rental
		where 
			deleted >= last_update_dt
			or last_update >= last_update_dt;
		
		call staging.set_table_load_time('staging.rental', current_update_dt);
	end;

$$ language plpgsql;


---------------------Address_load in Staging---------------------------------
create or replace procedure staging.address_load(current_update_dt timestamp)
as $$
	begin	
		delete from staging.address;
	
		insert
		into
			staging.address
		  (   
			address_id,
			address,
			address2,
			district,
			city_id,
			postal_code,
			phone,
			last_update
			)

		select 
			address_id,
			address,
			address2,
			district,
			city_id,
			postal_code,
			phone,
			last_update
		from
			film_src.address ;
		
		call staging.set_table_load_time('staging.address', current_update_dt);
	end;
$$ language plpgsql;


---------------------City_load in Staging---------------------------------
create or replace procedure staging.city_load(current_update_dt timestamp)
as $$
	begin	
		delete from staging.city;
		insert
		into staging.city			
		   (
			city_id,
			city,
			country_id,
			last_update
			)
		select 
			city_id,
			city,
			country_id,
			last_update
		from
			film_src.city ;
		
		call staging.set_table_load_time('staging.city', current_update_dt);
	end;
$$ language plpgsql;


---------------------Staff_load in Staging---------------------------------
create or replace procedure staging.staff_load(current_update_dt timestamp)
as $$
	begin	
		delete from staging.staff;
		insert
		into staging.staff			
		(
			staff_id,
			first_name,
			last_name,
			address_id,
			email,
			store_id,
			active,
			username,
			"password",
			last_update,
			picture,
			deleted
		)
		select 
			staff_id,
			first_name,
			last_name,
			address_id,
			email,
			store_id,
			active,
			username,
			"password",
			last_update,
			picture,
			deleted
		from
			film_src.staff  ;
		
		call staging.set_table_load_time('staging.staff', current_update_dt);
	end;
$$ language plpgsql;

---------------------Store_load in Staging---------------------------------
create or replace procedure staging.store_load(current_update_dt timestamp)
as $$
	begin	
		delete from staging.store;
		insert
		into staging.store	
			(
			store_id,
			manager_staff_id,
			address_id,
			last_update
			)
		select 
			store_id,
			manager_staff_id,
			address_id,
			last_update
		from
			film_src.store ;
		
		call staging.set_table_load_time('staging.store', current_update_dt);
	end;
$$ language plpgsql;


---------------------Payment_load in Staging---------------------------------
create or replace procedure staging.payment_load(current_update_dt timestamp)
as $$
	declare 
		last_update_dt timestamp;
	begin
		last_update_dt = staging.get_last_update_table('staging.payment');
	
		delete from staging.payment;

		insert into staging.payment	
			(
			payment_id,
			customer_id,
			staff_id,
			rental_id,
			amount,
			payment_date,
			last_update,
			deleted
			)	
		select 
			payment_id,
			customer_id,
			staff_id,
			rental_id,
			amount,
			payment_date,
			last_update,
			deleted
		from
			film_src.payment
		where 
			deleted >= last_update_dt
			or last_update >= last_update_dt;
		
		call staging.set_table_load_time('staging.payment', current_update_dt);
	end;

$$ language plpgsql;

/*
call staging.film_load(now()::timestamp);
call staging.rental_load(now()::timestamp);
call staging.inventory_load(now()::timestamp);
select * from staging.film f;
select * from staging.rental r ;
select count(*) from staging.rental r ;
select * from staging.inventory i ;
select count(*) from film_src.rental r ;*/

-------------------------------------------------------------------------------------------------------------------------------------------------
--Создание таблиц ODS слоя 
-------------------------------------------------------------------------------------------------------------------------------------------------

------Film-------------------
drop table if exists ods.film;
CREATE TABLE ods.film (
	film_id int2 NOT NULL,
	title varchar(255) NOT NULL,
	description text NULL,
	release_year public."year" NULL,
	language_id int2 NOT NULL,
	rental_duration int2 NOT NULL,
	rental_rate numeric(4, 2) NOT NULL,
	length int2 NULL,
	replacement_cost numeric(5, 2) NOT NULL,
	rating public."mpaa_rating" NULL ,
	last_update timestamp NOT NULL,
	special_features _text NULL,
	fulltext tsvector NOT NULL
);

--------------Inventory------------
drop table if exists ods.inventory;
CREATE TABLE ods.inventory (
	inventory_id int4 NOT NULL,
	film_id int2 NOT NULL,
	store_id int2 NOT NULL,
	last_update timestamp NOT NULL ,
	deleted timestamp NULL
);

------------Rental--------------
drop table if exists ods.rental;
CREATE TABLE ods.rental (
	rental_id int4 NOT NULL,
	rental_date timestamp NOT NULL,
	inventory_id int4 NOT NULL,
	customer_id int2 NOT NULL,
	return_date timestamp NULL,
	staff_id int2 NOT NULL,
	last_update timestamp NOT NULL,
	deleted timestamp NULL
);

-------Address--------------------
drop table if exists ods.address;
CREATE TABLE ods.address (
	address_id int4 NOT NULL,
	address varchar(50) NOT NULL,
	address2 varchar(50) NULL,
	district varchar(20) NOT NULL,
	city_id int2 NOT NULL,
	postal_code varchar(10) NULL,
	phone varchar(20) NOT NULL,
	last_update timestamp NOT NULL 
);

----------City----------------
drop table if exists ods.city;
CREATE TABLE ods.city (
	city_id int2 NOT NULL,
	city varchar(50) NOT NULL,
	country_id int2 NOT NULL,
	last_update timestamp NOT NULL 
);

------------------Staff--------
drop table if exists ods.staff;
CREATE TABLE ods.staff (
	staff_id int2 NOT NULL,
	first_name varchar(45) NOT NULL,
	last_name varchar(45) NOT NULL,
	address_id int2 NOT NULL,
	email varchar(50) NULL,
	store_id int2 NOT NULL,
	active bool NOT NULL ,
	username varchar(16) NOT NULL,
	"password" varchar(40) NULL,
	last_update timestamp NOT NULL ,
	picture bytea NULL,
	deleted timestamp NULL
);

---------------Store--------------
DROP TABLE if exists ods.store;
CREATE TABLE ods.store (
	store_id int2 NOT NULL,
	manager_staff_id int2 NOT NULL,
	address_id int2 NOT NULL,
	last_update timestamp NOT NULL 
);

--------Payment-----------------
DROP TABLE if exists ods.payment;
CREATE TABLE ods.payment (
	payment_id int4 NOT NULL,
	customer_id int2 NOT NULL,
	staff_id int2 NOT NULL,
	rental_id int4 NOT NULL,
	amount numeric(5, 2) NOT NULL,
	payment_date timestamp NOT NULL,
	last_update timestamp NOT NULL,
	deleted timestamp NULL
);

------------------------------------------------------------------------------------------------------------------------------------------------
---- Создание процедур загрузки из Staging in ODS слой 
------------------------------------------------------------------------------------------------------------------------------------------------

-----Film_load in Ods from Staging -------
create or replace procedure ods.film_load()
 as $$
	begin
		delete from ods.film;

		insert
		into ods.film
			(
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
			)
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
			staging.film;
	end;
$$ language plpgsql;

-----Inventory_load in Ods from Staging -------
create or replace procedure ods.inventory_load()
as $$
	begin	
		delete from ods.inventory;

		insert into ods.inventory
		(
			inventory_id, 
			film_id, 
			store_id,
			last_update,
			deleted			
		)
		select 
			inventory_id, 
			film_id, 
			store_id,
			last_update,
			deleted
		from
			staging.inventory i;
		
	end;
$$ language plpgsql;


-----Rental_load in Ods from Staging -------
create or replace procedure ods.rental_load()
as $$

	begin
		delete from ods.rental odr
		where odr.rental_id in (
			select 
				sr.rental_id 
		    from 
		    	staging.rental sr
		    );

		insert into ods.rental
		(
			rental_id, 
			rental_date, 
			inventory_id, 
			customer_id, 
			return_date, 
			staff_id,
			last_update,
			deleted
		)
		select 
			rental_id, 
			rental_date, 
			inventory_id, 
			customer_id, 
			return_date, 
			staff_id,
			last_update,
			deleted
		from
			staging.rental;
	end;

$$ language plpgsql;

-----Address_load in Ods from Staging -------
create or replace procedure ods.address_load()
as $$
	begin	
		delete from ods.address;
	
		insert
		into
			ods.address
		  (   
			address_id,
			address,
			address2,
			district,
			city_id,
			postal_code,
			phone,
			last_update
			)

		select 
			address_id,
			address,
			address2,
			district,
			city_id,
			postal_code,
			phone,
			last_update
		from
			staging.address ;
		
	end;
$$ language plpgsql;


-----City_load in Ods from Staging -------
create or replace procedure ods.city_load()
as $$
	begin	
		delete from ods.city;
		insert
		into ods.city			
		   (
			city_id,
			city,
			country_id,
			last_update
			)
		select 
			city_id,
			city,
			country_id,
			last_update
		from
			staging.city ;
		
	end;
$$ language plpgsql;


-----Staff_load in Ods from Staging -------
create or replace procedure ods.staff_load()
as $$
	begin	
		delete from ods.staff;
		insert
		into ods.staff			
		(
			staff_id,
			first_name,
			last_name,
			address_id,
			email,
			store_id,
			active,
			username,
			"password",
			last_update,
			picture,
			deleted
		)
		select 
			staff_id,
			first_name,
			last_name,
			address_id,
			email,
			store_id,
			active,
			username,
			"password",
			last_update,
			picture,
			deleted
		from
			staging.staff  ;
		
	end;
$$ language plpgsql;

-----Store_load in Ods from Staging --------
create or replace procedure ods.store_load()
as $$
	begin	
		delete from ods.store;
		insert
		into ods.store	
			(
			store_id,
			manager_staff_id,
			address_id,
			last_update
			)
		select 
			store_id,
			manager_staff_id,
			address_id,
			last_update
		from
			staging.store ;
		
	end;
$$ language plpgsql;


-----Payment_load in Ods from Staging -------
create or replace procedure ods.payment_load()
as $$

	begin
	
		delete from ods.payment odp
		where odp.payment_id in (
		    select 
		    	stp.payment_id 
		   	from 
		   		staging.payment stp
		   		) ;
		   	
		insert into ods.payment	
			(
			payment_id,
			customer_id,
			staff_id,
			rental_id,
			amount,
			payment_date,
			last_update,
			deleted
			)	
		select 
			payment_id,
			customer_id,
			staff_id,
			rental_id,
			amount,
			payment_date,
			last_update,
			deleted
		from
			staging.payment;

	end;
$$ language plpgsql;


------------------------------------------------------------------------------------------------------------------------------------------------
--Cоздание таблиц REF слоя 
------------------------------------------------------------------------------------------------------------------------------------------------


---------Film-----------------
drop table if exists ref.film;
CREATE TABLE ref.film (
	film_sk serial4 not null,
	film_nk  int2 NOT NULL
);

--------Inventory-----------------
drop table if exists ref.inventory;
CREATE TABLE ref.inventory (
	inventory_sk serial4 NOT null,
	inventory_nk int4 NOT NULL
);

-------Rental--------------------
drop table if exists ref.rental;
CREATE TABLE ref.rental (
	rental_sk serial4 NOT null,
	rental_nk int4 NOT NULL
);

--------Address-----------------
drop table if exists ref.address;
CREATE TABLE ref.address (
	address_sk serial4 not null,
	address_nk  int2 NOT NULL
);

-------City-------------------
drop table if exists ref.city;
CREATE TABLE ref.city (
	city_sk serial4 not null,
	city_nk  int2 NOT NULL
);

-----------Staff---------------
drop table if exists ref.staff;
CREATE TABLE ref.staff (
	staff_sk serial4 not null,
	staff_nk  int2 NOT NULL
);

----------Store----------------
drop table if exists ref.store;
CREATE TABLE ref.store (
	store_sk serial4 not null,
	store_nk  int2 NOT NULL
);

-----------Payment--------------
drop table if exists ref.payment;
CREATE TABLE ref.payment (
	payment_sk serial4 not null,
	payment_nk  int4 NOT NULL
);

-------------------------------------------------------------------------------------------------------------------------------------------------
----- Cоздаем процедуры наполнения REF
-------------------------------------------------------------------------------------------------------------------------------------------------

----------Film_sync--------------------------
create or replace procedure ref.film_id_sync()
as $$
begin 
	insert into ref.film (
	film_nk
	)	
	select 
		f.film_id 
	from 
		ods.film f
	left join ref.film rf 
		on f.film_id = rf.film_nk 
	where rf.film_nk is null 
    order by f.film_id ;
	
end;
$$ language plpgsql;

----------Inventory_sync--------------------------
create or replace procedure ref.inventory_id_sync()
as $$
begin 
	insert into ref.inventory (
	inventory_nk
	)	
	select 
		i.inventory_id 
	from 
		ods.inventory i
	left join ref.inventory ri 
		on i.inventory_id = ri.inventory_nk  
	where ri.inventory_nk  is null 
    order by i.inventory_id;	
end;
$$ language plpgsql;


----------Rental_sync--------------------------
create or replace procedure ref.rental_id_sync()
as $$
begin 
	insert into ref.rental (
	rental_nk
	)	
	select 
		r.rental_id 		
	from 
		ods.rental r
	left join ref.rental rr 
		on r.rental_id = rr.rental_nk  
	where rr.rental_nk  is null 
    order by r.rental_id;	
end;
$$ language plpgsql;


----------Adress_sync--------------------------
create or replace procedure ref.address_id_sync()
as $$
begin 
	insert into ref.address (
	address_nk 
	)	
	select 
		a.address_id
	from 
		ods.address a
	left join ref.address ra 
		on a.address_id = ra.address_nk  
	where ra.address_nk is null 
    order by a.address_id ;
	
end;
$$ language plpgsql;



----------City_sync--------------------------
create or replace procedure ref.city_id_sync()
as $$
begin 
	insert into ref.city (
	city_nk
	)	
	select 
		c.city_id 
	from 
		ods.city c 
	left join ref.city rc 
		on c.city_id = rc.city_nk
	where rc.city_nk  is null 
    order by c.city_id  ;
	
end;
$$ language plpgsql;


----------Staff_sync--------------------------
create or replace procedure ref.staff_id_sync()
as $$
begin 
	insert into ref.staff (
	staff_nk
	)	
	select 
		s.staff_id 
	from 
		ods.staff s 
	left join ref.staff rs  
		on s.staff_id  = rs.staff_nk 
	where rs.staff_nk  is null 
    order by s.staff_id ;
	
end;
$$ language plpgsql;


----------Store_sync--------------------------
create or replace procedure ref.store_id_sync()
as $$
begin 
	insert into ref.store (
	store_nk
	)	
	select 
		s.store_id 
	from 
		ods.store s 
	left join ref.store rs  
		on s.store_id = rs.store_nk 
	where rs.store_nk  is null 
    order by s.store_id ;
	
end;
$$ language plpgsql;



----------Payment_sync--------------------------
create or replace procedure ref.payment_id_sync()
as $$
begin 
	insert into ref.payment (
	payment_nk
	)	
	select 
		p.payment_id  
	from 
		ods.payment p  
	left join ref.payment rp  
		on p.payment_id = rp.payment_nk 
	where rp.payment_nk is null 
    order by p.payment_id ;
	
end;
$$ language plpgsql;


-------------------------------------------------------------------------------------------------------------------------------------------------
---------------FULL LOAD PROCEDURE--------------
CREATE OR REPLACE PROCEDURE full_load()
 LANGUAGE plpgsql
AS $procedure$
    declare 
		current_update_dt timestamp = now();
	begin
		call staging.film_load(current_update_dt);
		call staging.inventory_load(current_update_dt);
		call staging.rental_load(current_update_dt);
		call staging.address_load(current_update_dt);
		call staging.city_load(current_update_dt);
	    call staging.staff_load(current_update_dt);
	    call staging.store_load(current_update_dt );
	   	call staging.payment_load(current_update_dt );
	    
		call ods.film_load();
		call ods.inventory_load();
		call ods.rental_load();
		call ods.address_load();
	    call ods.city_load();
	    call ods.staff_load();
	    call ods.store_load();
	    call ods.payment_load();
	
		call ref.film_id_sync();
		call ref.inventory_id_sync();
		call ref.rental_id_sync();
		call ref.address_id_sync();
		call ref.city_id_sync();
		call ref.staff_id_sync();
	    call ref.store_id_sync();
	    call ref.payment_id_sync();
	   		
	end;
$procedure$
;
-------------------------------------------------------------------------------------------------------------------------------------------------
call  full_load();
