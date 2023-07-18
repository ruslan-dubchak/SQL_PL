-- core.fact_payment definition

-- Drop table

DROP table if exists core.fact_payment;
CREATE TABLE core.fact_payment (
	payment_pk int4 NOT NULL,
	amount numeric(8, 2) NOT NULL,
	cnt int2 NOT NULL,
	payment_date date NOT NULL,
	inventory_fk integer not null references dim_inventory(inventory_pk)
	staff_fk integer not null references dim_staff(staff_pk) ,
	
	PRIMARY KEY (payment_pk)
);


-- core.dim_inventory definition

-- Drop table
DROP TABLE if exists core.dim_inventory;
CREATE TABLE core.dim_inventory (
	inventory_pk int4 NOT NULL,
	inventory_id int4 NOT NULL,
	film_id int4 NOT NULL,
	title varchar(255) NOT NULL,
	rental_duration int2 NOT NULL,
	rental_rate numeric(4, 2) NOT NULL,
	length int2 NULL,
	rating varchar(10) NULL,
	PRIMARY KEY (inventory_pk)
);


-- core.dim_staff definition

-- Drop table

--Создать таблицу измерения dim_staff, куда включить информацию по сотруднику и магазину.

DROP TABLE if exists core.dim_staff;
CREATE TABLE core.dim_staff (
	staff_pk int4 NOT NULL,
	staff_id int4 NOT NULL,
	first_name varchar(45) NOT NULL,
	last_name varchar(45) NOT NULL,
	adress_store varchar(50) NOT NULL,
	city_store varchar(50) NOT NULL,
	coutry_store varchar(50) NOT NULL,
    PRIMARY KEY (staff_pk)
);


--Создать таблицу фактов fact_rental (измерение по customer создавать не надо).


DROP table if exists core.fact_rental;
CREATE TABLE core.fact_rental(
	rental_pk int4 NOT NULL,
	amount_disck int4 NOT NULL,
	rental_date date NOT NULL,
	inventory_fk integer not null references dim_inventory(inventory_pk),
	staff_fk integer not null references dim_staff(staff_pk),
	
	PRIMARY KEY (rental_pk)
);




