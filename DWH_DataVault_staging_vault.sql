create schema staging;

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


------------------------------------------------------------------------------------------------------------------------------------------------
                                          -- Создание таблиц Data Vault слоя--
------------------------------------------------------------------------------------------------------------------------------------------------

drop table if exists HubFilm;
create table HubFilm(
	HubFilmHashKey varchar (32) primary key,
	LoadDate timestamp not null,
	RecordSource varchar(50) not null,
	FilmID int2 not null
);

drop table if exists HubInventory;
create table HubInventory(
	HubInventoryHashKey varchar (32) primary key,
	LoadDate timestamp not null,
	RecordSource varchar(50) not null,
	InventoryID int2 not null
);


drop table if exists HubRental;
create table HubRental(
	HubRentalHashKey varchar (32) primary key,
	LoadDate timestamp not null,
	RecordSource varchar(50) not null,
	RentalID int2 not null
);


drop table if exists HubPayment;
create table HubPayment(
	HubPaymentHashKey varchar (32) primary key,
	LoadDate timestamp not null,
	RecordSource varchar(50) not null,
	PaymentID int4 not null
);


drop table if exists HubAddress;
create table HubAddress(
	HubAddressHashKey varchar (32) primary key,
	LoadDate timestamp not null,
	RecordSource varchar(50) not null,
	AddressID int4 not null
);


drop table if exists HubStore;
create table HubStore(
	HubStoreHashKey varchar (32) primary key,
	LoadDate timestamp not null,
	RecordSource varchar(50) not null,
	StoreID int4 not null
);

drop table if exists HubStaff;
create table HubStaff(
	HubStaffHashKey varchar (32) primary key,
	LoadDate timestamp not null,
	RecordSource varchar(50) not null,
	StaffID int4 not null
);

drop table if exists HubCity;
create table HubCity(
	HubCityHashKey varchar (32) primary key,
	LoadDate timestamp not null,
	RecordSource varchar(50) not null,
	CityID int4 not null
);
----Создаем Линки ---


drop table if exists LinkFilmInventory;
create table LinkFilmInventory(
	LinkFilmInventoryHashKey varchar (32) primary key,
	LoadDate timestamp not null,
	RecordSource varchar(50) not null,
	HubFilmHashKey varchar (32) references HubFilm (HubFilmHashKey), 
	HubInventoryHashKey varchar (32) references HubInventory (HubInventoryHashKey)
);

drop table if exists LinkRentalInventory;
create table LinkRentalInventory(
	LinkRentalInventoryHashKey varchar (32) primary key,
	LoadDate timestamp not null,
	RecordSource varchar(50) not null,
	HubRentalHashKey varchar (32) references HubRental (HubRentalHashKey), 
	HubInventoryHashKey varchar (32) references HubInventory (HubInventoryHashKey)
);


drop table if exists LinkPaymentRental;
create table LinkPaymentRental(
	LinkPaymentRentalHashKey varchar (32) primary key,
	LoadDate timestamp not null,
	RecordSource varchar(50) not null,
	HubPaymentHashKey varchar (32) references HubPayment (HubPaymentHashKey), 
	HubRentalHashKey varchar (32) references HubRental (HubRentalHashKey)
);

drop table if exists LinkInventoryStore;
create table LinkInventoryStore(
	LinkInventoryStoreHashKey varchar (32) primary key,
	LoadDate timestamp not null,
	RecordSource varchar(50) not null,
	HubInventoryHashKey varchar (32) references HubInventory (HubInventoryHashKey), 
	HubStoreHashKey varchar (32) references HubStore (HubStoreHashKey)
);


drop table if exists LinkRentalStaff;
create table LinkRentalStaff(
	LinkRentalStaffHashKey varchar (32) primary key,
	LoadDate timestamp not null,
	RecordSource varchar(50) not null,
	HubRentalHashKey varchar (32) references HubRental (HubRentalHashKey), 
	HubStaffHashKey varchar (32) references HubStaff (HubStaffHashKey)
);

drop table if exists LinkPaymentStaff;
create table LinkPaymentStaff(
	LinkPaymentStaffHashKey varchar (32) primary key,
	LoadDate timestamp not null,
	RecordSource varchar(50) not null,
	HubPaymentHashKey varchar (32) references HubPayment (HubPaymentHashKey), 
	HubStaffHashKey varchar (32) references HubStaff (HubStaffHashKey)
);


drop table if exists LinkAddressStore;
create table LinkAddressStore(
	LinkAddressStoreHashKey varchar (32) primary key,
	LoadDate timestamp not null,
	RecordSource varchar(50) not null,
	HubAddressHashKey varchar (32) references HubAddress (HubAddressHashKey), 
	HubStoreHashKey varchar (32) references HubStore (HubStoreHashKey)
);

drop table if exists LinkStaffStoreManager;
create table LinkStaffStoreManager(
	LinkStaffStoreHashKey varchar (32) primary key,
	LoadDate timestamp not null,
	RecordSource varchar(50) not null,
	HubStaffHashKey varchar (32) references HubStaff (HubStaffHashKey), 
	HubStoreHashKey varchar (32) references HubStore (HubStoreHashKey)
);

drop table if exists LinkAddressCity;
create table LinkAddressCity(
	LinkAddressCityHashKey varchar (32) primary key,
	LoadDate timestamp not null,
	RecordSource varchar(50) not null,
	HubAddressHashKey varchar (32) references HubAddress (HubAddressHashKey), 
	HubCityHashKey varchar (32) references HubCity (HubCityHashKey)
);



----Создаем Сателиты ---

drop table if exists SatFilm;
create table SatFilm(
 HubFilmHashKey varchar (32) not null references HubFilm (HubFilmHashKey),
 LoadDate timestamp not null,
 LoadEndDate timestamp not null,
 RecordSource varchar(50) not null,
 HashDiff varchar (32) not null,
 
 Title varchar (255),
 Description text,
 ReleaseYear year,
 Length int2,
 Rating mpaa_rating,
 
 PRIMARY KEY (HubFilmHashKey,LoadDate)
);

drop table if exists SatFilmMon;
create table SatFilmMon(
 HubFilmHashKey varchar (32) not null references HubFilm (HubFilmHashKey),
 LoadDate timestamp not null,
 LoadEndDate timestamp not null,
 RecordSource varchar(50) not null,
 HashDiff varchar (32) not null,
 
 RentalDuration int2,
 RentalRate numeric(4,2) ,
 ReplacementCost numeric(4,2), 

 
 PRIMARY KEY (HubFilmHashKey,LoadDate)
);



drop table if exists SatFilmInventory;
create table SatFilmInventory(
 LinkFilmInventoryHashKey varchar (32) not null references LinkFilmInventory (LinkFilmInventoryHashKey) ,
 LoadDate timestamp not null,
 LoadEndDate timestamp not null,
 RecordSource varchar(50) not null,

 PRIMARY KEY (LinkFilmInventoryHashKey,LoadDate)
);

drop table if exists SatInventory;
create table SatInventory(
 HubInventoryHashKey varchar (32) not null references HubInventory (HubInventoryHashKey),
 LoadDate timestamp not null,
 LoadEndDate timestamp not null,
 RecordSource varchar(50) not null,

 PRIMARY KEY (HubInventoryHashKey,LoadDate)
);


drop table if exists SatRentalInventory;
create table SatRentalInventory(
 LinkRentalInventoryHashKey varchar (32) not null references LinkRentalInventory (LinkRentalInventoryHashKey),
 LoadDate timestamp not null,
 LoadEndDate timestamp not null,
 RecordSource varchar(50) not null,

 PRIMARY KEY (LinkRentalInventoryHashKey,LoadDate)
);


drop table if exists SatRentalDate;
create table SatRentalDate(
 HubRentalHashKey varchar (32) not null references HubRental (HubRentalHashKey),
 LoadDate timestamp not null,
 LoadEndDate timestamp not null,
 RecordSource varchar(50) not null,
 
 RentalDate timestamp,

 PRIMARY KEY (HubRentalHashKey,LoadDate)
);

drop table if exists SatRentalReturnDate;
create table SatRentalReturnDate(
 HubRentalHashKey varchar (32) not null references HubRental (HubRentalHashKey),
 LoadDate timestamp not null,
 LoadEndDate timestamp not null,
 RecordSource varchar(50) not null,
 
 RentalReturnDate timestamp,

 PRIMARY KEY (HubRentalHashKey,LoadDate)
);

drop table if exists SatPaymentRental;
create table SatPaymentRental(
 LinkPaymentRentalHashKey varchar (32) not null references LinkPaymentRental (LinkPaymentRentalHashKey),
 LoadDate timestamp not null,
 LoadEndDate timestamp not null,
 RecordSource varchar(50) not null,

 PRIMARY KEY (LinkPaymentRentalHashKey,LoadDate)
);

drop table if exists SatPayment;
create table SatPayment(
 HubPaymentHashKey varchar (32) not null references HubPayment (HubPaymentHashKey),
 LoadDate timestamp not null,
 LoadEndDate timestamp not null,
 RecordSource varchar (50) not null,
 HashDiff varchar (32) not null,
 
 Amount numeric (5,2) ,
 PaymentDate timestamp, 

 PRIMARY KEY (HubPaymentHashKey,LoadDate)
);


drop table if exists SatInventoryStore;
create table SatInventoryStore(
 LinkInventoryStoreHashKey varchar (32) not null references LinkInventoryStore (LinkInventoryStoreHashKey),
 LoadDate timestamp not null,
 LoadEndDate timestamp not null,
 RecordSource varchar (50) not null,
 
 PRIMARY KEY (LinkInventoryStoreHashKey,LoadDate)
);


drop table if exists SatRentalStaff;
create table SatRentalStaff(
 LinkRentalStaffHashKey varchar (32) not null references LinkRentalStaff (LinkRentalStaffHashKey),
 LoadDate timestamp not null,
 LoadEndDate timestamp not null,
 RecordSource varchar (50) not null,
 
 PRIMARY KEY (LinkRentalStaffHashKey,LoadDate)
);


drop table if exists SatPaymentStaff;
create table SatPaymentStaff(
 LinkPaymentStaffHashKey varchar (32) not null references LinkPaymentStaff (LinkPaymentStaffHashKey),
 LoadDate timestamp not null,
 LoadEndDate timestamp not null,
 RecordSource varchar (50) not null,
 
 PRIMARY KEY (LinkPaymentStaffHashKey,LoadDate)
);


drop table if exists SatAddressStore;
create table SatAddressStore(
 LinkAddressStoreHashKey varchar (32) not null references LinkAddressStore (LinkAddressStoreHashKey),
 LoadDate timestamp not null,
 LoadEndDate timestamp not null,
 RecordSource varchar (50) not null,
 
 PRIMARY KEY (LinkAddressStoreHashKey,LoadDate)
);


drop table if exists SatAddress;
create table SatAddress(
 HubAddressHashKey varchar (32) not null references HubAddress (HubAddressHashKey),
 LoadDate timestamp not null,
 LoadEndDate timestamp not null,
 RecordSource varchar (50) not null,
 HashDiff varchar (32) not null,
 
 Address varchar (50),
 Address2 varchar (50), 
 District varchar (20),
 PostalCode varchar (10),

 PRIMARY KEY (HubAddressHashKey,LoadDate)
);

drop table if exists SatAddressR;
create table SatAddressR(
 HubAddressHashKey varchar (32) not null references HubAddress (HubAddressHashKey),
 LoadDate timestamp not null,
 LoadEndDate timestamp not null,
 RecordSource varchar (50) not null,
 
 Phone varchar (20),

 PRIMARY KEY (HubAddressHashKey,LoadDate)
);


drop table if exists SatCity;
create table SatCity(
 HubCityHashKey varchar (32) not null references HubCity (HubCityHashKey),
 LoadDate timestamp not null,
 LoadEndDate timestamp not null,
 RecordSource varchar (50) not null,
 
 City varchar (50),

 PRIMARY KEY (HubCityHashKey,LoadDate)
);

drop table if exists SatAddressCity;
create table SatAddressCity(
 LinkAddressCityHashKey varchar (32) not null references LinkAddressCity (LinkAddressCityHashKey),
 LoadDate timestamp not null,
 LoadEndDate timestamp not null,
 RecordSource varchar (50) not null,
 
 PRIMARY KEY (LinkAddressCityHashKey,LoadDate)
);


drop table if exists SatStore;
create table SatStore(
 HubStoreHashKey varchar (32) not null references HubStore (HubStoreHashKey),
 LoadDate timestamp not null,
 LoadEndDate timestamp not null,
 RecordSource varchar (50) not null,
 
 PRIMARY KEY (HubStoreHashKey,LoadDate)
);


drop table if exists SatStaffStoreManager;
create table SatStaffStoreManager(
 LinkStaffStoreHashKey varchar (32) not null references LinkStaffStoreManager (LinkStaffStoreHashKey),
 LoadDate timestamp not null,
 LoadEndDate timestamp not null,
 RecordSource varchar (50) not null,
 
 PRIMARY KEY (LinkStaffStoreHashKey,LoadDate)
);

drop table if exists SatStaff;
create table SatStaff(
 HubStaffHashKey varchar (32) not null references HubStaff (HubStaffHashKey),
 LoadDate timestamp not null,
 LoadEndDate timestamp not null,
 RecordSource varchar (50) not null,
 HashDiff varchar (32) not null,
 
 FirstName varchar (45),
 LastName varchar (45), 
 Email varchar (50),
 Username varchar (16),

 PRIMARY KEY (HubStaffHashKey,LoadDate)
);


drop table if exists SatStaffQ;
create table SatStaffQ(
 HubStaffHashKey varchar (32) not null references HubStaff (HubStaffHashKey),
 LoadDate timestamp not null,
 LoadEndDate timestamp not null,
 RecordSource varchar (50) not null,

 Password varchar (40),

 PRIMARY KEY (HubStaffHashKey,LoadDate)
);

drop table if exists SatStaffR;
create table SatStaffR(
 HubStaffHashKey varchar (32) not null references HubStaff (HubStaffHashKey),
 LoadDate timestamp not null,
 LoadEndDate timestamp not null,
 RecordSource varchar (50) not null,

 Active bool ,

 PRIMARY KEY (HubStaffHashKey,LoadDate)
);
/*
call staging.film_load(now()::timestamp);
call staging.rental_load(now()::timestamp);
call staging.inventory_load(now()::timestamp);
select * from staging.film f;
select * from staging.rental r ;
select count(*) from staging.rental r ;
select * from staging.inventory i ;
select count(*) from film_src.rental r ;*/


create or replace procedure full_load()
as $$
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
	    	
end;

$$ language plpgsql;

call full_load()