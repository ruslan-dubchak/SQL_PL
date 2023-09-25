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
--------------INTEGRETOIN------------------------------
-------------------------------------------------------------------------------------------------------------------------------------------------
--- Создание и наполнение INTEGRETION слоя

--create schema integ;


------Film-------------------
drop table if exists integ.film;
CREATE TABLE integ.film (
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
drop table if exists integ.inventory;
CREATE TABLE integ.inventory (
	inventory_id int4 NOT NULL,
	film_id int2 NOT NULL,
	store_id int2 NOT NULL,
	last_update timestamp NOT NULL ,
	deleted timestamp NULL
);

------------Rental--------------
drop table if exists integ.rental;
CREATE TABLE integ.rental (
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
drop table if exists integ.address;
CREATE TABLE integ.address (
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
drop table if exists integ.city;
CREATE TABLE integ.city (
	city_id int2 NOT NULL,
	city varchar(50) NOT NULL,
	country_id int2 NOT NULL,
	last_update timestamp NOT NULL 
);

------------------Staff--------
drop table if exists integ.staff;
CREATE TABLE integ.staff (
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
DROP TABLE if exists integ.store;
CREATE TABLE integ.store (
	store_id int2 NOT NULL,
	manager_staff_id int2 NOT NULL,
	address_id int2 NOT NULL,
	last_update timestamp NOT NULL 
);

--------Payment-----------------
DROP TABLE if exists integ.payment;
CREATE TABLE integ.payment (
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
---- Создание процедур загрузки в Integ слой 
------------------------------------------------------------------------------------------------------------------------------------------------

-------Film_integ load
create or replace procedure integ.film_load()
as $$
begin 
	delete from integ.film;

	insert into integ.film
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
		rf.film_sk as film_id,
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
		ods.film f 
		join "ref".film rf on rf.film_nk = f.film_id; 		
end;
$$ language plpgsql;


-------Inventory_load in Integretion--------------
create or replace procedure integ.inventory_load()
as $$
begin 
	delete from integ.inventory;

	insert into integ.inventory 
    (		
		inventory_id,
		film_id,
		store_id,
		last_update,
		deleted
	)
	select 
		ri.inventory_sk as inventory_id,
		rf.film_sk as film_id,
		rs.store_sk as store_id,
		last_update,
		deleted
	from 
		ods.inventory i  
		join "ref".inventory ri 
			on ri.inventory_nk = i.inventory_id 
		join "ref".film rf 
			on rf.film_nk = i.film_id 
		join ref.store rs 
			on rs.store_nk = i.store_id ; 		
end;
$$ language plpgsql;


---------------------Rental_load in Integration ---------------------------------
create or replace procedure integ.rental_load()
as $$
	declare 
		last_update_dt timestamp;
	begin
		--дата и время последней изменненной записи , загруженнной в предудущий раз 
		last_update_dt = (
			select coalesce ( max(r.last_update), '1900-01-01'::date) 
			from integ.rental r
			);
		
		-- индикаторы всех созданных , измененных или удаленных строк с предыдущей загрузки из одс в интеграшин 
		create temporary table updated_integ_rent_id_list on commit drop as 
		select 
			r.rental_id 
		from
			ods.rental r 
		where r.last_update >last_update_dt;
	
	--удаляем из integ слоя все созданные , изминенные или удаленнные факты сдачи в аренду с предыдущей загрузки из ods в integ
		delete from integ.rental r 
		where
			r.rental_id in (
				select 
					rental_id 
				from updated_integ_rent_id_list
			);
----вставляем в  integ слоя все созданные , изминенные или удаленнные факты сдачи в аренду с предыдущей загрузки из ods в integ
		insert into integ.rental
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
			rr.rental_sk as rental_id,
			rental_date,
			ri.inventory_sk as inventory_id,
			customer_id,
			return_date,
			rs.staff_sk as staff_id,
			last_update,
			deleted
		from ods.rental r 
		join "ref".rental rr 
			on rr.rental_nk =r.rental_id 
		join  updated_integ_rent_id_list upd
			on upd.rental_id  = r.rental_id
		join "ref".inventory ri 
			on ri.inventory_nk = r.inventory_id
		join ref.staff rs 
			on rs.staff_nk = r.staff_id ;

	end;
$$ language plpgsql;

-------Address_load in Integretion--------------
create or replace procedure integ.address_load()
as $$
begin 
	delete from integ.address;

	insert into integ.address 
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
		ra.address_sk as address_id,
		address,
		address2,
		district,
		rc.city_sk as city_id,
		postal_code,
		phone,
		last_update
	from 
		ods.address a  
	join "ref".address ra
		on ra.address_nk = a.address_id 
	join "ref".city rc 
		on rc.city_nk = a.city_id ;
	 		
end;
$$ language plpgsql;

-------City_load in Integretion--------------
create or replace procedure integ.city_load()
as $$
begin 
	delete from integ.city;

	insert into integ.city  
    (		
		city_id,
		city,
		country_id,
		last_update
	)
	select 
		rc.city_sk as city_id,
		city,
		country_id,
		last_update
	from 
		ods.city c  
	join "ref".city rc 
		on rc.city_nk = c.city_id ;
	 		
end;
$$ language plpgsql;

-------Staff_load in Integretion--------------
create or replace procedure integ.staff_load()
as $$
begin 
	delete from integ.staff;

	insert into integ.staff  
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
		rs.staff_sk as staff_id,
		first_name,
		last_name,
		ra.address_sk as address_id,
		email,
		rss.store_sk as store_id,
		active,
		username,
		"password",
		last_update,
		picture,
		deleted
	from 
		ods.staff s  
	join "ref".staff rs 
		on rs.staff_nk = s.staff_id
	join "ref".address ra 
		on ra.address_nk = s.address_id
	join "ref".store rss 
		on rss.store_nk = s.store_id;
	 		
end;
$$ language plpgsql;

-------Store_load in Integretion--------------
create or replace procedure integ.store_load()
as $$
begin 
	delete from integ.store;

	insert into integ.store  
    (		
		store_id,
		manager_staff_id,
		address_id,
		last_update
	)
	select 
		rss.store_sk as store_id,
		manager_staff_id,
		ra.address_sk as address_id,
		last_update
	from 
		ods.store s  
	join "ref".store rss 
		on rss.store_nk = s.store_id
	join "ref".address ra 
		on ra.address_nk = s.address_id ;
	 		
end;
$$ language plpgsql;

---------------------Payment_load in Integration ---------------------------------
create or replace procedure integ.payment_load()
as $$
	declare 
		last_update_dt timestamp;
	begin
		--- дата и время последней изминенной записи , загруженной в последний раз 
		last_update_dt = (
			select coalesce ( max(p.last_update), '1900-01-01'::date) 
			from integ.payment p
			);
		
		-- идентификаторы всех созданных , изминенных или удаленных платежей с предыдущей загрузки из ОДС в ИНТ
		create temporary table updated_integ_pay_id_list on commit drop as 
		select 
			p.payment_id  
		from
			ods.payment p  
		where p.last_update >last_update_dt;
	
	--- удаляем из integ все соданные , изминенные или удаленные платежи с предыдущей загрузки из ОДС в ИНТ
		delete from integ.payment p 
		where
			p.payment_id  in (
				select 
					payment_id  
				from updated_integ_pay_id_list
			);
--- вствляем  из integ все соданные , изминенные или удаленные платежи с предыдущей загрузки из ОДС в ИНТ
		insert into integ.payment 
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
			rp.payment_sk as payment_id,
			customer_id,
			rs.staff_sk as staff_id,
			rr.rental_sk as rental_id,
			amount,
			payment_date,
			last_update,
			deleted
		from ods.payment p  
		join ref.payment rp 
			on rp.payment_nk = p.payment_id 
		join "ref".rental rr 
			on rr.rental_nk =p.rental_id 
		join  updated_integ_pay_id_list upp
			on upp.payment_id  = p.payment_id 
		join ref.staff rs 
			on rs.staff_nk = p.staff_id ;

	end;
$$ language plpgsql;

select *  from ods.payment p 


--------------------------------------------------------------------------------------------------------------------------------------------
--------------DDS - Реализуем истричность 
-------------------------------------------------------------------------------------------------------------------------------------------------
--- Создание и наполнение DDS слоя

---create schema dds;


------Film-------------------
drop table if exists dds.film;
CREATE TABLE dds.film (
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
	special_features _text NULL,
	fulltext tsvector NOT null,
	
	date_effective_from timestamp not null,
	date_effective_to timestamp not null,
	is_active boolean not null,
	
	hash varchar(32)
);

--------------Inventory------------
drop table if exists dds.inventory;
CREATE TABLE dds.inventory (
	inventory_id int4 NOT NULL,
	film_id int2 NOT NULL,
	store_id int2 NOT NULL,

	
	date_effective_from timestamp not null,
	date_effective_to timestamp not null,
	is_active boolean not null,
	
	hash varchar(32)
);

------------Rental--------------
drop table if exists dds.rental;
CREATE TABLE dds.rental (
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
drop table if exists dds.address;
CREATE TABLE dds.address (
	address_id int4 NOT NULL,
	address varchar(50) NOT NULL,
	address2 varchar(50) NULL,
	district varchar(20) NOT NULL,
	city_id int2 NOT NULL,
	postal_code varchar(10) NULL,
	phone varchar(20) NOT NULL,
	
	date_effective_from timestamp not null,
	date_effective_to timestamp not null,
	is_active boolean not null,
	
	hash varchar(32)
);

----------City----------------
drop table if exists dds.city;
CREATE TABLE dds.city (
	city_id int2 NOT NULL,
	city varchar(50) NOT NULL,
	country_id int2 NOT NULL,
	
	date_effective_from timestamp not null,
	date_effective_to timestamp not null,
	is_active boolean not null,
	
	hash varchar(32)
);

------------------Staff--------
drop table if exists dds.staff;
CREATE TABLE dds.staff (
	staff_id int2 NOT NULL,
	first_name varchar(45) NOT NULL,
	last_name varchar(45) NOT NULL,
	address_id int2 NOT NULL,
	email varchar(50) NULL,
	store_id int2 NOT NULL,
	active bool NOT NULL ,
	username varchar(16) NOT NULL,
	"password" varchar(40) NULL,
	picture bytea NULL,
	
	date_effective_from timestamp not null,
	date_effective_to timestamp not null,
	is_active boolean not null,
	
	hash varchar(32)
);

---------------Store--------------
DROP TABLE if exists dds.store;
CREATE TABLE dds.store (
	store_id int2 NOT NULL,
	manager_staff_id int2 NOT NULL,
	address_id int2 NOT NULL,
	
	date_effective_from timestamp not null,
	date_effective_to timestamp not null,
	is_active boolean not null,
	
	hash varchar(32)
);

--------Payment-----------------
DROP TABLE if exists dds.payment;
CREATE TABLE dds.payment (
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
---- Создание процедур загрузки в DDS слой 
------------------------------------------------------------------------------------------------------------------------------------------------
-------Film_dds load
create or replace procedure dds.film_load()
as $$
begin 
	
	--- список id новых фильмов 
	create temporary table film_new_id_list on commit drop as 
	select 
		rf.film_sk as film_id
	from 
		"ref".film rf  
		left join dds.film f
			on rf.film_sk = f.film_id
	where 
		f.film_id is null;
	
	----- вставляем новые фильмы в DDS 
insert into dds.film
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
		special_features,
		fulltext,
		date_effective_from,
		date_effective_to,
		is_active,
		hash
	)
	select
		f.film_id,
		title,
		description,
		release_year,
		language_id,
		rental_duration,
		rental_rate,
		length,
		replacement_cost,
		rating,
		special_features,
		fulltext,
		
		'1900-01-01'::date as date_effective_from,
		'9999-01-01':: date as date_effective_to,
		true as  is_active,
		
		md5(f::text) as  hash --- это Хэш , получаем от строки , всех строк из Integ как md5(f::text)
	from 
		integ.film 	f 
		join film_new_id_list nf 
			on f.film_id = nf.film_id; 
 
----Получаем список всех удаленных
    create temporary table film_deleted_id_list on commit drop as  
		select 
			f.film_id 
		from 
			dds.film f 
			left join integ.film inf 
				on f.film_id = inf.film_id 
		where inf.film_id is null;

	--помечаем наши удаленные фильмы 
	update dds.film f 
	set 
		is_active = false,
		date_effective_to = now()
	from 
		film_deleted_id_list fd 
	where 
		fd.film_id = f.film_id 
		and f.is_active is true;
		
	--- находим id  измененных фильмов 
	create temporary table film_update_id_list on commit drop as 	
	select 
		inf.film_id 
	from 
		dds.film f
		join integ.film inf 	
			on f.film_id = inf.film_id 
	where
		f.is_active is true 
		and f.hash <> md5(inf::text) ;
	
	
	---- помечаем неактуальными предыдущие строки по изминенным фильмам
	update dds.film f 
	set 
		is_active = false,
		date_effective_to = inf.last_update 
	from 
		integ.film inf 
		join film_update_id_list upf
			on upf.film_id = inf.film_id 
	where 
		inf.film_id = f.film_id 
		and f.is_active is true;
	
	---добавляем новые строки по измененным фильмам 
	insert into dds.film
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
		special_features,
		fulltext,
		date_effective_from,
		date_effective_to,
		is_active,
		hash
	)
	select
		f.film_id,
		title,
		description,
		release_year,
		language_id,
		rental_duration,
		rental_rate,
		length,
		replacement_cost,
		rating,
		special_features,
		fulltext,
		
		last_update as date_effective_from,
		'9999-01-01':: date as date_effective_to,
		true as  is_active,
		
		md5(f::text) as  hash 
	from 
		integ.film 	f 
		join film_update_id_list upf  
			on f.film_id = upf.film_id; 
		
end;
$$ LANGUAGE plpgsql ;


-------Inventory_dds-- load
create or replace procedure dds.inventory_load()
as $$
begin 
	
	--- список id новых компакт дисков 
	create temporary table inventory_new_id_list on commit drop as 
	select 
		ri.inventory_sk  as inventory_id
	from 
		"ref".inventory ri   
		left join dds.inventory i 
			on ri.inventory_sk = i.inventory_id 
	where 
		i.inventory_id  is null;
	
	----- вставляем новые фильмы в DDS 
insert into dds.inventory 
   ( 
		inventory_id,
		film_id,
		store_id,
		
		date_effective_from,
		date_effective_to,
		is_active,
		
		hash
	)
	select
		i.inventory_id,
		film_id,
		store_id,
		
		'1900-01-01'::date as date_effective_from,
		'9999-01-01':: date as date_effective_to,
		true as  is_active,
		
		md5(i::text) as  hash --- это Хэш , получаем от строки , всех строк из Integ как md5(f::text)
	from 
		integ.inventory i 
		join inventory_new_id_list nl 
			on i.inventory_id  = nl.inventory_id; 
 
----Получаем список всех удаленных
    create temporary table inventory_deleted_id_list on commit drop as  
		select 
			ini.deleted ,
			i.inventory_id  
		from 
			dds.inventory i 
			left join integ.inventory ini 
				on i.inventory_id = ini.inventory_id 
		where ini.inventory_id  is null;

	--помечаем наши удаленные компакт диски  
	update dds.inventory i 
	set 
		is_active = false,
		date_effective_to = id.deleted
	from 
		inventory_deleted_id_list id 
	where 
		id.inventory_id = i.inventory_id  
		and i.is_active is true;
		
	--- находим id  измененных компакт дисков  
	create temporary table inventory_update_id_list on commit drop as 	
	select 
		ini.inventory_id 
	from 
		dds.inventory i 
		join integ.inventory ini 	
			on i.inventory_id  = ini.inventory_id  
	where
		i.is_active is true 
		and i.hash <> md5(ini::text) ;
	
	
	---- помечаем неактуальными предыдущие строки по изминенным компакт дискам
	update dds.inventory i  
	set 
		is_active = false,
		date_effective_to = ini.last_update 
	from 
		integ.inventory ini  
		join inventory_update_id_list upl
			on upl.inventory_id = ini.inventory_id  
	where 
		ini.inventory_id = i.inventory_id 
		and i.is_active is true;
	
	---добавляем новые строки по измененным компакт дискам
	insert into dds.inventory 
   ( 
		inventory_id,
		film_id,
		store_id,
		
		date_effective_from,
		date_effective_to,
		is_active,
		
		hash
	)
	select
		i.inventory_id,
		film_id,
		store_id,
		
		last_update as date_effective_from,
		'9999-01-01':: date as date_effective_to,
		true as  is_active,
		
		md5(i::text) as  hash 
	from 
		integ.inventory i  
		join inventory_update_id_list upl  
			on i.inventory_id  = upl.inventory_id; 
		
end;
$$ LANGUAGE plpgsql ;


------Rental_dds--load----------------------
create or replace procedure dds.rental_load()
as $$
	declare 
		last_update_dt timestamp;
	begin
		--дата и время последней изменненной записи , загруженнной в предудущий раз 
		last_update_dt = (
			select coalesce ( max(r.last_update), '1900-01-01'::date) 
			from dds.rental r
			);
		
		-- индикаторы всех созданных , измененных или удаленных строк с предыдущей загрузки из инт в ддс 
		create temporary table updated_dds_rent_id_list on commit drop as 
		select 
			r.rental_id 
		from
			integ.rental r 
		where r.last_update >last_update_dt;
	
	--удаляем из dds слоя все созданные , изминенные или удаленнные факты сдачи в аренду с предыдущей загрузки из integ в dds
		delete from dds.rental r 
		where
			r.rental_id in (
				select 
					rental_id 
				from updated_dds_rent_id_list
			);
----вставляем в  dds слоя все созданные , изминенные или удаленнные факты сдачи в аренду с предыдущей загрузки из integ в dds
		insert into dds.rental
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
			r.rental_id ,
			rental_date,
			inventory_id,
			customer_id,
			return_date,
			staff_id,
			last_update,
			deleted
		from integ.rental r 
		join  updated_dds_rent_id_list upd
			on upd.rental_id  = r.rental_id ;

	end;
$$ language plpgsql;


-------address_dds-- load
create or replace procedure dds.address_load()
as $$
begin 
	
	--- список id новых adress 
	create temporary table address_new_id_list on commit drop as 
	select 
		ra.address_sk  as address_id
	from 
		"ref".address ra   
		left join dds.address a  
			on ra.address_sk  = a.address_id  
	where 
		a.address_id  is null;
	
	----- вставляем новые фильмы в DDS 
insert into dds.address 
   ( 
		address_id,
		address,
		address2,
		district,
		city_id,
		postal_code,
		phone,
		
		date_effective_from,
		date_effective_to,
		is_active,
		
		hash
	)
	select
		a.address_id,
		address,
		address2,
		district,
		city_id,
		postal_code,
		phone,
		
		'1900-01-01'::date as date_effective_from,
		'9999-01-01':: date as date_effective_to,
		true as  is_active,
		
		md5(a::text) as  hash --- это Хэш , получаем от строки , всех строк из Integ как md5(f::text)
	from 
		integ.address a 
		join address_new_id_list nl 
			on a.address_id  = nl.address_id; 
 
----Получаем список всех удаленных
    create temporary table address_deleted_id_list on commit drop as  
		select 
			a.address_id  
		from 
			dds.address a  
			left join integ.address ina
				on a.address_id = ina.address_id
		where ina.address_id is null;

	--помечаем наши удаленные address  
	update dds.address a 
	set 
		is_active = false,
		date_effective_to = now()
	from 
		address_deleted_id_list ad 
	where 
		ad.address_id = a.address_id  
		and a.is_active is true;
		
	--- находим id  измененных adreesov  
	create temporary table address_update_id_list on commit drop as 	
	select 
		ina.address_id 
	from 
		dds.address a 
		join integ.address ina	
			on a.address_id = ina.address_id 
	where
		a.is_active is true 
		and a.hash <> md5(ina::text) ;
	
	
	---- помечаем неактуальными предыдущие строки по изминенным adress
	update dds.address a 
	set 
		is_active = false,
		date_effective_to = ina.last_update 
	from 
		integ.address ina
		join address_update_id_list upl
			on upl.address_id = ina.address_id  
	where 
		ina.address_id = a.address_id  
		and a.is_active is true;
	
	---добавляем новые строки по измененным компакт дискам
	insert into dds.address 
   ( 
		address_id,
		address,
		address2,
		district,
		city_id,
		postal_code,
		phone,
		
		date_effective_from,
		date_effective_to,
		is_active,
		
		hash
	)
	select
		a.address_id,
		address,
		address2,
		district,
		city_id,
		postal_code,
		phone,
		
		last_update as date_effective_from,
		'9999-01-01':: date as date_effective_to,
		true as  is_active,
		
		md5(a::text) as  hash 
	from 
		integ.address a  
		join address_update_id_list upl  
			on a.address_id = upl.address_id; 
		
end;
$$ LANGUAGE plpgsql ;


-------city_dds-- load
create or replace procedure dds.city_load()
as $$
begin 
	
	--- список id новых adress 
	create temporary table city_new_id_list on commit drop as 
	select 
		rc.city_sk  as city_id
	from 
		"ref".city rc   
		left join dds.city c  
			on rc.city_sk = c.city_id  
	where 
		c.city_id is null;
	
	----- вставляем новые фильмы в DDS 
insert into dds.city  
   ( 
		city_id,
		city,
		country_id,
		
		date_effective_from,
		date_effective_to,
		is_active,
		
		hash
	)
	select
		c.city_id,
		city,
		country_id,
		
		'1900-01-01'::date as date_effective_from,
		'9999-01-01':: date as date_effective_to,
		true as  is_active,
		
		md5(c::text) as  hash --- это Хэш , получаем от строки , всех строк из Integ как md5(f::text)
	from 
		integ.city c  
		join city_new_id_list nl 
			on c.city_id = nl.city_id; 
 
----Получаем список всех удаленных
    create temporary table city_deleted_id_list on commit drop as  
		select 
			c.city_id  
		from 
			dds.city c  
			left join integ.city inc
				on c.city_id = inc.city_id 
		where inc.city_id  is null;

	--помечаем наши удаленные city  
	update dds.city c
	set 
		is_active = false,
		date_effective_to = now()
	from 
		city_deleted_id_list cd 
	where 
		cd.city_id = c.city_id  
		and c.is_active is true;
		
	--- находим id  измененных city  
	create temporary table city_update_id_list on commit drop as 	
	select 
		inc.city_id 
	from 
		dds.city c  
		join integ.city inc 
			on c.city_id = inc.city_id 
	where
		c.is_active is true 
		and c.hash <> md5(inc::text) ;
	
	
	---- помечаем неактуальными предыдущие строки по изминенным city
	update dds.city c
	set 
		is_active = false,
		date_effective_to = inc.last_update  
	from 
		integ.city inc
		join city_update_id_list upl
			on upl.city_id = inc.city_id 
	where 
		inc.city_id = c.city_id  
		and c.is_active is true;
	
	---добавляем новые строки по измененным компакт city
	insert into dds.city 
   ( 
		city_id,
		city,
		country_id,
		
		date_effective_from,
		date_effective_to,
		is_active,
		
		hash
	)
	select
		c.city_id,
		city,
		country_id,
		
		last_update as date_effective_from,
		'9999-01-01':: date as date_effective_to,
		true as  is_active,
		
		md5(c::text) as  hash 
	from 
		integ.city c  
		join city_update_id_list upl  
			on c.city_id = upl.city_id; 
		
end;
$$ LANGUAGE plpgsql ;


-------staff_DDS--load
create or replace procedure dds.staff_load()
as $$
begin 
	
	--- список id новых adress 
	create temporary table staff_new_id_list on commit drop as 
	select 
		rs.staff_sk  as staff_id
	from 
		"ref".staff rs  
		left join dds.staff s  
			on rs.staff_sk = s.staff_id  
	where 
		s.staff_id  is null;
	
	----- вставляем новые фильмы в DDS 
insert into dds.staff  
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
		picture,
		
		date_effective_from,
		date_effective_to,
		is_active,
		
		hash
	)
	select
		s.staff_id,
		first_name,
		last_name,
		address_id,
		email,
		store_id,
		active,
		username,
		"password",
		picture,
		
		'1900-01-01'::date as date_effective_from,
		'9999-01-01':: date as date_effective_to,
		true as  is_active,
		
		md5(s::text) as  hash --- это Хэш , получаем от строки , всех строк из Integ как md5(f::text)
	from 
		integ.staff s 
		join staff_new_id_list nl 
			on s.staff_id = nl.staff_id; 
 
----Получаем список всех удаленных
    create temporary table staff_deleted_id_list on commit drop as  
		select 
			ins.deleted as deleted, 
			s.staff_id  
		from 
			dds.staff s  
			left join integ.staff ins
				on s.staff_id  = ins.staff_id 
		where ins.staff_id  is null;

	--помечаем наши удаленные staff 
	update dds.staff s
	set 
		is_active = false,
		date_effective_to = sd.deleted
	from 
		staff_deleted_id_list sd 
	where 
		sd.staff_id = s.staff_id  
		and s.is_active is true;
		
	--- находим id  измененных staff  
	create temporary table staff_update_id_list on commit drop as 	
	select 
		ins.staff_id 
	from 
		dds.staff s  
		join integ.staff ins  
			on s.staff_id  = ins.staff_id 
	where
		s.is_active is true 
		and s.hash <> md5(ins::text) ;
	

	---- помечаем неактуальными предыдущие строки по изминенным staff
	update dds.staff s
	set 
		is_active = false,
		date_effective_to = ins.last_update 
	from 
		integ.staff ins
		join staff_update_id_list upl
			on upl.staff_id = ins.staff_id  
	where 
		ins.staff_id = s.staff_id 
		and s.is_active is true;
	
	---добавляем новые строки по измененным компакт staff
	insert into dds.staff 
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
		picture,
		
		date_effective_from,
		date_effective_to,
		is_active,
		
		hash
	)
	select
		s.staff_id,
		first_name,
		last_name,
		address_id,
		email,
		store_id,
		active,
		username,
		"password",
		picture,
		
		last_update as date_effective_from,
		'9999-01-01'::date as date_effective_to,
		true as  is_active,
		
		md5(s::text) as  hash 
	from 
		integ.staff s  
		join staff_update_id_list upl  
			on s.staff_id  = upl.staff_id; 
		
end;
$$ LANGUAGE plpgsql ;




-------store__dds-- load
create or replace procedure dds.store_load()
as $$
begin 
	
	--- список id новых store
	create temporary table store_new_id_list on commit drop as 
	select 
		rs.store_sk  as store_id
	from 
		"ref".store rs  
		left join dds.store s 
			on rs.store_sk = s.store_id  
	where 
		s.store_id is null;
	
	----- вставляем новые  в DDS 
insert into dds.store  
   ( 
		store_id,
		manager_staff_id,
		address_id,
		
		date_effective_from,
		date_effective_to,
		is_active,
		
		hash
	)
	select
		s.store_id,
		manager_staff_id,
		address_id,
		
		'1900-01-01'::date as date_effective_from,
		'9999-01-01':: date as date_effective_to,
		true as  is_active,
		
		md5(s::text) as  hash --- это Хэш , получаем от строки , всех строк из Integ как md5(f::text)
	from 
		integ.store s  
		join store_new_id_list nl 
			on s.store_id  = nl.store_id; 
 
----Получаем список всех удаленных
    create temporary table store_deleted_id_list on commit drop as  
		select 
			s.store_id 
		from 
			dds.store s  
			left join integ.store ins
				on s.store_id = ins.store_id 
		where ins.store_id  is null;

	--помечаем наши удаленные store 
	update dds.store s
	set 
		is_active = false,
		date_effective_to = now()
	from 
		store_deleted_id_list sd 
	where 
		sd.store_id = s.store_id 
		and s.is_active is true;
		
	--- находим id  измененных store
	create temporary table store_update_id_list on commit drop as 	
	select 
		ins.store_id  
	from 
		dds.store s  
		join integ.store ins 
			on s.store_id = ins.store_id 
	where
		s.is_active is true 
		and s.hash <> md5(ins::text) ;
	
	
	---- помечаем неактуальными предыдущие строки по изминенным store
	update dds.store s
	set 
		is_active = false,
		date_effective_to = ins.last_update  
	from 
		integ.store ins
		join store_update_id_list upl
			on upl.store_id = ins.store_id  
	where 
		ins.store_id = s.store_id  
		and s.is_active is true;
	
	---добавляем новые строки по измененным компакт store_
	insert into dds.store  
   ( 
		store_id,
		manager_staff_id,
		address_id,
		
		date_effective_from,
		date_effective_to,
		is_active,
		
		hash
	)
	select
		s.store_id,
		manager_staff_id,
		address_id,
		
		last_update as date_effective_from,
		'9999-01-01':: date as date_effective_to,
		true as  is_active,
		
		md5(s::text) as  hash 
	from 
		integ.store s 
		join store_update_id_list upl  
			on s.store_id = upl.store_id; 
		
end;
$$ LANGUAGE plpgsql ;




---------------------Payment_load in DDS ---------------------------------
create or replace procedure dds.payment_load()
as $$
	declare 
		last_update_dt timestamp;
	begin
		--- дата и время последней изминенной записи , загруженной в последний раз 
		last_update_dt = (
			select coalesce ( max(p.last_update), '1900-01-01'::date) 
			from dds.payment p
			);
		
		-- идентификаторы всех созданных , изминенных или удаленных платежей с предыдущей загрузки из int в dds
		create temporary table updated_dds_pay_id_list on commit drop as 
		select 
			p.payment_id  
		from
			integ.payment p  
		where p.last_update >last_update_dt;
	
	--- удаляем из dds все соданные , изминенные или удаленные платежи с предыдущей загрузки из  int в dds
		delete from dds.payment p 
		where
			p.payment_id  in (
				select 
					payment_id  
				from updated_dds_pay_id_list
			);
--- вствляем  из dds все соданные , изминенные или удаленные платежи с предыдущей загрузки из int в dds
		insert into dds.payment 
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
			p.payment_id,
			customer_id,
			staff_id,
			rental_id,
			amount,
			payment_date,
			last_update,
			deleted
		from integ.payment p  
		join  updated_dds_pay_id_list upp
			on upp.payment_id  = p.payment_id ;

	end;
$$ language plpgsql;

select *  from dds.payment p 


-------------------------------------------------------------------------------------------------------------------------------------------------
                                            -- REPORT--
-------------------------------------------------------------------------------------------------------------------------------------------------
--create schema report; Cоздание слоя для отчета ( Data Mart)

--------------Sales_by_date---------------
drop table if exists report.sales_by_date;
CREATE TABLE report.sales_by_date (
	sales_date_rn int not null,
	sales_date_title varchar(50) not null,
	amount float not null	
);

drop table if exists report.calendar;
create table report.calendar (
	date_id int4 NOT NULL,
	date_actual date NOT NULL,
	day_of_month int4 NOT NULL,
	month_name varchar(9) not null,
	year int4 NOT NULL
);

drop table if exists report.sales_by_film;
create table report.sales_by_film (
	sales_film_rn int not null,
	sales_film_title varchar(255) not null,
	amount float not null
);

--------------------------Процедуры наполнения Report -------------------------------------------------------------------------------------------


-------------------Calendar Load------------------------------------------
create or replace procedure  report.fill_calendar (sdate date, nm integer)
as $$ 
	begin 
		SET lc_time = 'ru_RU';
		
		INSERT INTO report.calendar
		SELECT TO_CHAR(datum, 'yyyymmdd')::INT AS date_id,
		       datum AS date_actual,
		       EXTRACT(DAY FROM datum) AS day_of_month,
		       TO_CHAR(datum, 'TMMonth') AS month_name,
		       EXTRACT(YEAR FROM datum) AS year
	   FROM (
			select
				sdate + SEQUENCE.DAY as datum
			from
				GENERATE_SERIES(0, nm - 1) as sequence (day)
			order by
				SEQUENCE.day
				) DQ			
		ORDER BY 1;		
	end;
	
$$ language plpgsql;




----------Наполнение витрины продаж по дням --- 
create or replace procedure  report.sales_by_date_calc()
as $$
	begin 
	 delete from report.sales_by_date;
		insert into report.sales_by_date	
		    (   
			sales_date_rn,
			sales_date_title,
			amount
			)
			select 
				c.date_id as sales_date_rn,
				concat(c.day_of_month,' ' ,c.month_name,' ', c."year") as sales_date_title,
				sum(p.amount) as  amount
			from dds.payment p
			join report.calendar c 
				on c.date_actual = p.payment_date::date 
			where
				p.deleted is null
			group by 
				c.date_id,
				concat(c.day_of_month,' ' ,c.month_name,' ', c."year");
	end;
	
$$ language plpgsql;


----------Наполнение витрины продаж по фильмам --- 
create or replace procedure  report.sales_by_film_calc()
as $$
	begin 
	 delete from report.sales_by_film;
		insert into report.sales_by_film 
				(
				sales_film_rn,
				sales_film_title,
				amount
				)
	        select 
	        	f.film_id as sales_film_rn,
	        	f.title as sales_film_title,
				sum (p.amount) as amount
			from dds.payment p
			join dds.rental r on p.rental_id = r.rental_id 	
			join dds.inventory i on i.inventory_id = r.inventory_id
			join dds.film f on f.film_id = i.film_id 
			where
				p.deleted is null 
				and r.deleted is null
			group by 
				f.film_id,
				f.title;
				
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
	   
	   call integ.film_load();
	   call integ.inventory_load();
	   call integ.rental_load();
	   call integ.address_load();
	   call integ.city_load();
	   call integ.staff_load();
	   call integ.store_load();
	   call integ.payment_load();
	  
	   call dds.film_load();
	   call dds.inventory_load();
	   call dds.rental_load();
	   call dds.address_load();
	   call	dds.city_load();
	   call dds.staff_load();
	   call dds.store_load();
	   call dds.payment_load();
	  
	  call report.sales_by_date_calc();
	  call report.sales_by_film_calc();
	end;
$procedure$
;

---создание функций-- 
create or replace function sales_by_date() returns table (sales_date_rn int , sales_date_title varchar, amount float)
as $$
	select 
		s.sales_date_rn,
		s.sales_date_title, 
		s.amount 
	from 
		report.sales_by_date s
$$ LANGUAGE sql;


DROP FUNCTION sales_by_film();
create or replace function sales_by_film() returns table ( sales_film_title varchar, amount float)
as $$
	select 
		sales_film_title, 
		amount 
	from 
		report.sales_by_film
$$ LANGUAGE sql;
-------------------------------------------------------------------------------------------------------------------------------------------------
call report.fill_calendar ('2005-01-01'::date, 6938)
call  full_load();

select * from sales_by_film()

