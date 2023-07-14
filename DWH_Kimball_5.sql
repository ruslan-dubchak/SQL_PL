DROP TABLE if exists staging.inventory;
CREATE TABLE staging.inventory (
	inventory_id int  NOT NULL,
	film_id int2 NOT NULL,
	store_id int2 NOT NULL,
	last_update timestamp NOT NULL 
);


drop procedure if exists staging.inventory_load();
create procedure staging.inventory_load() 
as $$
begin
delete from staging.inventory;
insert
into
staging.inventory
	(inventory_id,
	film_id,
	store_id, 
	last_update)

select 
	inventory_id,
	film_id,
	store_id, 
	last_update
from
	film_src.inventory;
end;
$$ language plpgsql;

call staging.inventory_load();

select * 
from staging.inventory;


DROP table if exists staging.rental;
CREATE TABLE staging.rental (
	rental_id int NOT NULL,
	rental_date timestamp NOT NULL,
	inventory_id int4 NOT NULL,
	customer_id int2 NOT NULL,
	return_date timestamp NULL,
	staff_id int2 NOT NULL,
	last_update timestamp NOT NULL 
);

drop procedure if exists staging.rental_load();
create procedure staging.rental_load() 
as $$
begin
delete from staging.rental;
insert
into
staging.rental
		(rental_id ,
		rental_date,
		inventory_id,
		customer_id,
		return_date,
		staff_id,
		last_update)

select 
		rental_id ,
		rental_date,
		inventory_id,
		customer_id,
		return_date,
		staff_id,
		last_update
from
	film_src.rental;
end;
$$ language plpgsql;

call staging.rental_load();

select * 
from staging.rental;



DROP table if exists staging.payment;
CREATE TABLE staging.payment (
	payment_id int NOT NULL,
	customer_id int2 NOT NULL,
	staff_id int2 NOT NULL,
	rental_id int4 NOT NULL,
	amount numeric(5, 2) NOT NULL,
	payment_date timestamp NOT NULL
);

drop procedure if exists staging.payment_load();
create procedure staging.payment_load() 
as $$
begin
delete from staging.payment;
insert
into
staging.payment
		(payment_id,
		customer_id,
		staff_id,
		rental_id,
		amount,
		payment_date)

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

call staging.payment_load();

select * 
from staging.payment;
