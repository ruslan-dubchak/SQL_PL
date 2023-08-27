select *
from staff s ;

--Инкриментальная загрузка данных : 
-- Создаем колонку в Источнике  deleted которая отслеживает удаление строк , то есть ставит флаг - удалился или нет 
alter table public.staff  add column deleted timestamp null;

-- Добавим поле в film_src

/*-- film_src.staff definition

-- Drop table

-- DROP FOREIGN TABLE film_src.staff;

CREATE FOREIGN TABLE film_src.staff (
	staff_id int4 OPTIONS(column_name 'staff_id') NOT NULL,
	first_name varchar(45) OPTIONS(column_name 'first_name') NOT NULL,
	last_name varchar(45) OPTIONS(column_name 'last_name') NOT NULL,
	address_id int2 OPTIONS(column_name 'address_id') NOT NULL,
	email varchar(50) OPTIONS(column_name 'email') NULL,
	store_id int2 OPTIONS(column_name 'store_id') NOT NULL,
	active bool OPTIONS(column_name 'active') NOT NULL,
	username varchar(16) OPTIONS(column_name 'username') NOT NULL,
	"password" varchar(40) OPTIONS(column_name 'password') NULL,
	last_update timestamp OPTIONS(column_name 'last_update') NOT NULL,
	picture bytea OPTIONS(column_name 'picture') NULL
)
SERVER film_pg
OPTIONS (schema_name 'public', table_name 'staff');*/

alter table film_src.staff  add column deleted timestamp OPTIONS(column_name 'deleted') null;

--в хранилище нет этой колонки deleted, надо добавить в также в  staging:

alter table staging.staff add column deleted timestamp null;


-- Так как таблица в Стейжинг слое у нас есть - которая отслеживает инкримент = создавать ее не будем. 
/*drop table if exists staging.last_update;
create table staging.last_update (
	table_name varchar (50) not null ,
	update_dt timestamp not null 
	);*/

--- cоздание инкримента 
create or replace  procedure staging.staff_load()
as $$
declare 
	last_update_dt timestamp;
begin
	last_update_dt = coalesce(
						(
						select  
							max(update_dt) 
						from  
							staging.last_update
						where 
						 	table_name = 'staging.staff'
						 ),
						'1900-01-01'::date
					 	);			 	
delete from staging.staff;
insert
into
staging.staff 
	(staff_id,
	first_name,
	last_name,
	store_id,
	deleted
	)
select 
    staff_id,
	first_name,
	last_name,
	store_id,
	deleted
from
	film_src.staff s 
where 
	s.last_update >=last_update_dt -- загружаем только строки котрорые были удалены после последнего ( максимального ) удаления или обновлены
	or 
	s.deleted >=last_update_dt 
	;
insert
	into staging.last_update
	(
	table_name,
	update_dt
	)
	values(
	'staging.staff',
	now()
);
end;
$$ language plpgsql;


--- отредактируем код загрузки в кор слой: 
--- Изначально добавим Uniq  в dimm_staff.staff_id ID - 
ALTER TABLE core.dim_staff 
ADD CONSTRAINT dim_staff_staff_id UNIQUE (staff_id);

create or replace  procedure core.load_staff()
as $$ 
begin 
	
delete from core.dim_staff  s
where s.staff_id  in (
select 
	stf.staff_id  
from 
	staging.staff  stf
where 
	stf.deleted  is not null
);
insert into core.dim_staff
   (
	staff_id,
	first_name,
	last_name,
	address,
	district,
	city_name
	)
select
	s.staff_id,
	s.first_name,
	s.last_name,
	a.address,
	a.district,
	c.city 
from
	staging.staff s
	join staging.store st using (store_id)
	join staging.address a using (address_id)
	join staging.city c using (city_id)
where 
	s.deleted is null
on conflict (staff_id) do update 
set 
	first_name = excluded.first_name,
	last_name =  excluded.last_name,
	address = excluded.address,
	district = excluded.district,
	city_name = excluded.city_name
;	
end;
$$ language plpgsql; 

--- 
call staging.staff_load();


---Полная загрузка ! 
call full_load ();

select * 
from core.dim_staff ds ;

select * 
from staging.last_update lu ;
