--- функция вызова таблицы - для замены в стейжингах 

create or replace function staging.get_last_update_table(table_name varchar ) returns  timestamp 
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



---- Добавляем историчность для талиц -  заносим последнее время изминения В ПРОЦЕДУРУ 
create or replace procedure staging.set_load_table_time( table_name varchar ,  current_update_dt timestamp default now())
as $$
	begin 
		insert
	    into staging.last_update
		(
		 table_name,
		 update_dt
		)
	    values(
		table_name,
		current_update_dt -- время начала загрузки данных 		
	     );
	end;
$$ language plpgsql;

--- alter table public.rental add column deleted timestamp;
--- alter table film_src.rental  add column deleted timestamp OPTIONS(column_name 'deleted') null;
--alter table staging.rental  add column last_update timestamp;
--delete from staging.rental; -- очистили 
--alter table staging.rental  alter column last_update set not null; --устаовили щграничения не нулевых 
--alter table staging.rental  add column deleted timestamp null;

CREATE OR REPLACE PROCEDURE staging.rental_load (current_update_dt timestamp)
 LANGUAGE plpgsql
AS $procedure$
declare 
	last_update_dt timestamp;
begin
	last_update_dt = staging.get_last_update_table('staging.rental'); --Вставили функцию вызова таблицы 
	
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
			film_src.rental r
		where
			r.last_update >=last_update_dt -- загружаем только строки котрорые были удалены после последнего ( максимального ) удаления или обновлены
			or 
			r.deleted >=last_update_dt;
		
		---- Добавляем историчность для талицы Фильм - заносим последнее время изминения 
         call staging.set_load_table_time('staging.rental', current_update_dt); -- время начала загрузки данных 
	end;

$procedure$
;


-- теперь добавим колонки в core.fact_rental: 
 -- effective_date_from and effective_date_to 
-- is_active == это флаг , что бы отбор можно было быстрый делать

/*alter table core.fact_rental add column effective_date_from timestamp default to_date('1900-01-01', 'yyyy-MM-hh') not null;--самое маое число 
alter table core.fact_rental  add column effective_date_to timestamp default to_date('9999-01-01', 'yyyy-MM-hh') not null;--самое большое 
alter table core.fact_rental  add column is_active boolean default true not null; -- флаг */


CREATE OR REPLACE PROCEDURE core.load_rental()
 LANGUAGE plpgsql
AS $procedure$
	begin 
		
		--отмечаем удаленные строки не активными 
	update core.fact_rental fr 
	set 
		is_active = false,
		effective_date_to = sr.deleted 
	from staging.rental sr 
	where 
		sr.deleted is not null
		and fr.rental_id = sr.rental_id
		and fr.is_active is true;
	
		-- получаем список идентификаторов новых фактов сдачи в аренду rental_id
	create temporary table new_rental_id_list  on commit drop as 
	select
		sr.rental_id 
    from
		staging.rental sr 
		left join core.fact_rental fr  using(rental_id)
	where 
			fr.rental_id  is null;	
		
		
		--- вставяем новые строки 
		insert into core.fact_rental
		(
			rental_id,
			inventory_fk,
			staff_fk,
			rental_date_fk,
			return_date_fk,
			effective_date_from,
			effective_date_to,
			is_active
		)
		select
			r.rental_id,
			i.inventory_pk as inventory_fk,
			s.staff_pk as staff_fk,
			dt_rental.date_dim_pk as rental_date_fk,
			dt_return.date_dim_pk as return_date_fk,
			r.last_update as effective_date_from,
	        coalesce (r.deleted,'9999-01-01'::date) as effective_date_to,
	        r.deleted is null  as is_active 
		from 
			new_rental_id_list idl 
			join staging.rental r 
				on r.rental_id = idl.rental_id		
			join core.dim_inventory i 
				on r.inventory_id = i.inventory_id  -- присоединяем только те кто входит в диапазон между обозгаченными датами в 
				and r.last_update  between i.effective_date_from and i.effective_date_to 
			join core.dim_staff s 
				on s.staff_id = r.staff_id
				and r.last_update between s.effective_date_from and s.effective_date_to  
			join core.dim_date dt_rental on dt_rental.date_actual = r.rental_date::date
		    left join core.dim_date dt_return on dt_return.date_actual = r.return_date::date;

	-- получаем список фактов сдачи в аренду индетификаторов по которым была только проставленна дата возврата 	   
   create temporary table update_return_id_list  on commit drop as 
		   select 
		   		r.rental_id 
		   from 
		   		staging.rental r 
		   		join core.fact_rental fr on fr.rental_id = r.rental_id 
		   		join core.dim_inventory di on fr.inventory_fk = di.inventory_pk 
		   		join core.dim_staff ds on fr.staff_fk = ds.staff_pk
		   		join core.dim_date dd on fr.rental_date_fk = dd.date_dim_pk 
		   		left join new_rental_id_list idl on idl.rental_id = r.rental_id 
		   where 
		   		r.return_date is not null
		   		and fr.return_date_fk is not null 
		   		and fr.is_active is true 
		   		and di.inventory_id = r.inventory_id 
		   		and ds.staff_id = r.staff_id 
		   		and dd.date_actual = r.rental_date ::date
		   		and r.deleted is null 
		   		and idl.rental_id is null;
		   	
	-- проставляем дату возврата у  фактов сдачи в аренду у которых была проставлена дата возврата  	
	update core.fact_rental r 
	set 
		return_date_fk = rd.date_dim_pk  
	from staging.rental sr 
		 join update_return_id_list urid on urid.rental_id = sr.rental_id 
		 join core.dim_date rd on rd.date_actual = sr.return_date ::date 	 
	where 
		r.rental_id = sr.rental_id
		and r.is_active is true ;
	
	--помечаем изменнненые факты не активными 
	update core.fact_rental r 
	set 
		is_active = false,
		effective_date_to = sr.last_update 
	from 
		staging.rental sr 
		left join update_return_id_list urid using (rental_id)
		left join new_rental_id_list nrid using (rental_id)
	where  
		sr.rental_id = r.rental_id 
		and r.is_active is true
		and urid.rental_id is null
		and nrid.rental_id is null
		and sr.deleted is null;
	
	-- вставляем изминненые данные 
			insert into core.fact_rental
		(
			rental_id,
			inventory_fk,
			staff_fk,
			rental_date_fk,
			return_date_fk,
			effective_date_from,
			effective_date_to,
			is_active
		)
		select
			r.rental_id,
			i.inventory_pk as inventory_fk,
			s.staff_pk as staff_fk,
			dt_rental.date_dim_pk as rental_date_fk,
			dt_return.date_dim_pk as return_date_fk,
			r.last_update as effective_date_from,
	        '9999-01-01'::date as effective_date_to,
	         true  as is_active 

		from  
			staging.rental r 
			join core.dim_inventory i 
				on r.inventory_id = i.inventory_id  -- присоединяем только те кто входит в диапазон между обозгаченными датами в 
				and r.last_update  between i.effective_date_from and i.effective_date_to 
			join core.dim_staff s 
				on s.staff_id = r.staff_id
				and r.last_update between s.effective_date_from and s.effective_date_to 
			left join core.dim_date dt_rental on dt_rental.date_actual = r.rental_date::date
		    left join core.dim_date dt_return on dt_return.date_actual = r.return_date::date
		    left join new_rental_id_list nrid using (rental_id)
		    left join update_return_id_list urid using (rental_id) 
		where 
			r.deleted is null
			and nrid.rental_id is null
			and urid.rental_id is null
			;
	
	end;
$procedure$
;





--- Пометим тблицу Фильм в стейжинг слое - добавим ее в отслеживание 

create or replace procedure staging.film_load(current_update_dt timestamp)
 as $$
	begin
		delete from staging.film;

		insert
		into
		staging.film
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
---- Добавляем историчность для талицы Фильм - заносим последнее время изминения 
         call staging.set_load_table_time('staging.film', current_update_dt); -- время начала загрузки данных 
	end;
$$ language plpgsql;



-- добавим начало загрузки в деколарировани процедуры 
create or replace  procedure staging.staff_load(current_update_dt timestamp)
as $$
declare 
	last_update_dt timestamp;
begin
	last_update_dt = staging.get_last_update_table('staging.staff'); --Вставили функцию вызова таблицы 
						
delete from staging.staff;
insert
into
staging.staff 
	(staff_id,
	first_name,
	last_name,
	store_id,
	last_update,-- добавили в загрузку данных //
	deleted
	)
select 
    staff_id,
	first_name,
	last_name,
	store_id,
	last_update,-- добавили в загрузку данных //
	deleted
from
	film_src.staff s 
where 
	s.last_update >=last_update_dt -- загружаем только строки котрорые были удалены после последнего ( максимального ) удаления или обновлены
	or 
	s.deleted >=last_update_dt 
	;
call staging.set_load_table_time ('staging.staff', current_update_dt); -- время начала загрузки данных 

end;
$$ language plpgsql;


-- добавим начало загрузки в деколарировани процедуры 
create or replace  procedure staging.inventory_load(current_update_dt timestamp) 
as $$
declare 
	last_update_dt timestamp;
begin
	last_update_dt = staging.get_last_update_table('staging.inventory'); --Вставили функцию вызова таблицы  

delete from staging.inventory;
insert
into
staging.inventory
	(inventory_id,
	film_id,
	store_id,
	last_update,-- добавили в загрузку данных //
	deleted
 
	)
select 
	inventory_id,
	film_id,
	store_id,
	last_update,-- добавили в загрузку данных //
	deleted
from
	film_src.inventory i 
where 
	i.last_update >=last_update_dt -- загружаем только строки котрорые были удалены после последнего ( максимального ) удаления или обновлены
	or 
	i.deleted >=last_update_dt 
	;
call staging.set_load_table_time ('staging.inventory', current_update_dt);-- время начала загрузки данных
end;
$$ language plpgsql;











create or replace  procedure core.load_inventory()
as $$ 
	declare 
		film_prev_update timestamp;
begin 

	-- помечаем удаленные записи: 
	update core.dim_inventory i 
	set 
		is_active = false,
		effective_date_to = si.deleted 
	from staging.inventory si 
	where 
		si.deleted is not null
		and i.inventory_id = si.inventory_id
		and i.is_active is true;
	
	-- получаем список идентификаторов новых компакт дисков
	create temporary table new_inventory_id_list  on commit drop as 
	select
			i.inventory_id 
		from
			staging.inventory i 
			left join core.dim_inventory di using(inventory_id)
		where 
			di.inventory_id is null;	
		
-- добавляем новые компакт диски в  dim_inventiry 
INSERT INTO core.dim_inventory
	(inventory_id, 
	film_id,
	title, 
	rental_duration, 
	rental_rate, 
	length, 
	rating,
	effective_date_from,
	effective_date_to,
	is_active)
select 
	i.inventory_id,
	i.film_id,
	f.title,
	f.rental_duration,
	f.rental_rate,
	f.length,
	f.rating,
	'1900-01-01'::date as effective_date_from,
	coalesce (i.deleted,'9999-01-01'::date) as effective_date_to,
	i.deleted is null  as is_active 
from 
	 staging.inventory i
join staging.film f using(film_id)
join new_inventory_id_list idl using (inventory_id);

-- помечаем измененные компакт диски не активными
update core.dim_inventory i 
set 
	is_active = false,
	effective_date_to = si.last_update 
from staging.inventory si
left join new_inventory_id_list idl using (inventory_id)
where 
	idl.inventory_id is null
	and si.deleted is null 
	and i.inventory_id = si.inventory_id 
	and i.is_active is true;

-- по измененым компакт дискам добавляем актуальные строки
INSERT INTO core.dim_inventory
	(inventory_id, 
	film_id,
	title, 
	rental_duration, 
	rental_rate, 
	length, 
	rating,
	effective_date_from,
	effective_date_to,
	is_active)
select 
	i.inventory_id,
	i.film_id,
	f.title,
	f.rental_duration,
	f.rental_rate,
	f.length,
	f.rating,
	i.last_update  as effective_date_from,
	'9999-01-01'::date as effective_date_to,
	true as is_active 
from 
	 staging.inventory i
join staging.film f using(film_id)
left join new_inventory_id_list idl using (inventory_id)
where 
	idl.inventory_id is null
	and i.deleted  is null;


--- историчность по Таблице Фильм :
-- получаем время предудущей загрузки в staging.film что бы получить измененные фильмы  
film_prev_update = (
	with lag_update as (
			 select 
			 	lag (lu.update_dt) over (order by lu.update_dt) as lag_update_dt
			 from 
			 	staging.last_update lu 
			 where 
			 	lu.table_name ='staging.film'
			 )
		select max(lag_update_dt) from lag_update
	    );

---получаем список изменненных фильмов  с момента предыдущей загрузки 	   
create temporary table updates_films on commit drop as 
	   select 
	   		f.film_id ,
	   		f.title ,
	   		f.rental_duration,
			f.rental_rate,
			f.length,
			f.rating,
			f.last_update   		
	   from staging.film f 
	   where f.last_update >=film_prev_update;
	  
--строки в dim_inventory которые нужно поменять 
create temporary table dim_inventory_rows_to_update on commit drop as 
	  select 
	  	di.inventory_pk,
	  	uf.last_update
	  from 
	  	core.dim_inventory di 
	  join updates_films uf 
	  	 on  uf.last_update > di.effective_date_from 
	  	 and uf.last_update < di.effective_date_to ;

-- Втавляем строки с новыми зачениями фильмов 
INSERT INTO core.dim_inventory
	(
	inventory_id, 
	film_id,
	title, 
	rental_duration, 
	rental_rate, 
	length, 
	rating,
	effective_date_from,
	effective_date_to,
	is_active
	)
	select 
		di.inventory_id ,
		di.film_id ,
		uf.title,
		uf.rental_duration,
		uf.rental_rate, 
		uf.length, 
		uf.rating,
		uf.last_update as effective_date_from,
		di.effective_date_to ,
		di.is_active 
	from 
		core.dim_inventory di 
		join dim_inventory_rows_to_update ru 
			on di.inventory_pk = ru.inventory_pk
		join updates_films uf 
			on uf.film_id = di.film_id;
		
	-- устанавливаем дату окончания действия строк для предыдущих  параметров фиьмов 	
update core.dim_inventory di 
set 
	effective_date_to = ru.last_update,
	is_active = false 
from 
	dim_inventory_rows_to_update ru  
where 
	ru.inventory_pk = di.inventory_pk ;
	
end;   
$$ language plpgsql; 



------------------------------------------------------------------------

call staging.full_load();

select * from staging.last_update lu ;

select count (*) from core.fact_rental fr ;

select count(*) from staging.rental r;
select count(*) from staging.inventory i;   
select count(*) from core.dim_inventory di ; 
select count(*) from staging.payment p  ;   
select count(*) from core.fact_payment fp  ; 
select max(dd.date_actual ),
	   min (dd.date_actual )
from core.dim_date dd 


select * 
from staging.rental r 
where r.rental_id =15894;

select * 
from core.fact_rental fr  
where fr.rental_id =15894;

select count (*) from core.fact_rental fr where fr.is_active is true ;

select
	*
from
	core.fact_rental fr
where
	fr.rental_id in (16117, 16116);
	
select * 
from 
	core.fact_rental fr  
order by 
	rental_pk desc	
	;
	
--------------------------------------------------------------
--внесем изминения на стороне источника в таблицу - last_update, deleted 

/*alter table public.payment add column last_update timestamp;*/

--update public.payment 
--set 
--	last_update = payment_date;

-- alter table public.payment alter column last_update set not null;
--alter table public.payment  add column deleted timestamp; 

--create trigger last_updated before
--update
--    on
--    public.rental for each row execute function last_updated();

--проверка last_update : 
/*update public.payment p
set 
	amount = 3.99
where p.payment_id = 32098 ;*/
---------------------------------------------------------------------------------------------------------------------------------------------
--внесем изминения в src  слой  : 
 --:film_src 
/*	CREATE FOREIGN TABLE film_src.payment (
	payment_id int4 OPTIONS(column_name 'payment_id') NOT NULL,
	customer_id int2 OPTIONS(column_name 'customer_id') NOT NULL,
	staff_id int2 OPTIONS(column_name 'staff_id') NOT NULL,
	rental_id int4 OPTIONS(column_name 'rental_id') NOT NULL,
	amount numeric(5, 2) OPTIONS(column_name 'amount') NOT NULL,
	payment_date timestamp OPTIONS(column_name 'payment_date') NOT NULL
)
SERVER film_pg
OPTIONS (schema_name 'public', table_name 'payment');*/

/*alter table film_src.payment  add column last_update timestamp OPTIONS(column_name 'last_update') NOT null;
alter table film_src.payment  add column deleted timestamp OPTIONS(column_name 'deleted') null;*/

---- внесем изминения в  staging слой :

/*delete from staging.payment ; -- очистили 

alter table staging.payment add column last_update timestamp not null;
alter table staging.payment add column deleted timestamp null;*/

--alter table staging.payment add column inventory_id int4 not null;
------------------------------------------------------------------------------------------------------------------------------------------------

-- изменим процедуру загрузки в staging.слой 
CREATE OR REPLACE PROCEDURE staging.payment_load(current_update_dt timestamp)
 LANGUAGE plpgsql
AS $procedure$
declare 
	last_update_dt timestamp;
begin
	last_update_dt = staging.get_last_update_table('staging.payment'); --Вставили функцию вызова таблицы 
	delete from staging.payment;

		insert into staging.payment
		(
			payment_id, 
			customer_id, 
			staff_id, 
			rental_id, 
			inventory_id,
			amount, 
			payment_date,
			last_update,
			deleted
		)
		select
			p.payment_id, 
			p.customer_id, 
			p.staff_id, 
			p.rental_id,
			r.inventory_id,
			p.amount, 
			p.payment_date,
			p.last_update,
			p.deleted
		from
			film_src.payment p
			join film_src.rental r using (rental_id)
		where
			p.last_update >=last_update_dt -- загружаем только строки котрорые были удалены после последнего ( максимального ) удаления или обновлены
			or p.deleted >=last_update_dt 
			or r.last_update >=last_update_dt;
		
		---- Добавляем историчность для талицы оплаты - заносим последнее время изминения 
         call staging.set_load_table_time('staging.payment', current_update_dt); -- время начала загрузки данных 
	end;
$procedure$
;

--call staging.payment_load (now()::timestamp);

--По д/ з нужно создать rental_id in table fact_payment, установить историчность по колонкам rental_id, amount , payment_date: 

--alter table core.fact_payment add column rental_id int4 not null;

-- теперь добавим колонки в core.fact_rental: 
 -- effective_date_from and effective_date_to 
-- is_active == это флаг , что бы отбор можно было быстрый делать
/*alter table core.fact_payment  add column effective_date_from timestamp default to_date('1900-01-01', 'yyyy-MM-hh') not null;--самое маое число 
alter table core.fact_payment  add column effective_date_to timestamp default to_date('9999-01-01', 'yyyy-MM-hh') not null;--самое большое 
alter table core.fact_payment  add column is_active boolean default true not null; -- флаг */

CREATE OR REPLACE PROCEDURE core.load_payment()
 LANGUAGE plpgsql
AS $procedure$
	begin
		--- отмечаем что удаленные строки не активны 
		update core.fact_payment p 
		set 
			is_active = false,
			effective_date_to = sp.deleted 
		from 
			staging.payment sp 
		where
		    sp.deleted is not null
		    and sp.payment_id = p.payment_id 
		    and p.is_active is true ;
			 
		--- получаем список идентификаторов новых платежей
			create temporary table new_payment_id_list  on commit drop as 
		select
			 p.payment_id 
	    from
			staging.payment p
			left join core.fact_payment fp using(payment_id)
		where 
				fp.payment_id  is null;	
			
		--- вставляем новые платежи 
		insert into core.fact_payment
		(
			payment_id,
			amount,
			payment_date_fk,
			inventory_fk,
			staff_fk,
			rental_id ,
			effective_date_from ,
			effective_date_to ,
			is_active 
		)
		select
			p.payment_id,
			p.amount,
			dt.date_dim_pk as payment_date_fk,
			di.inventory_pk as inventory_fk,
			ds.staff_pk as staff_fk,
			p.rental_id ,
			'1900-01-01'::date as effective_date_from,
			coalesce (p.deleted,'9999-01-01'::date) as effective_date_to,
			p.deleted is null  as is_active 
			
		from
			staging.payment p
			join new_payment_id_list np using (payment_id)
			join core.dim_inventory di 
				on p.inventory_id = di.inventory_id -- присоединяем только те кто входит в диапазон между обозгаченными датами в 
				and p.last_update  between di.effective_date_from and di.effective_date_to 
			join core.dim_staff ds
				on p.staff_id = ds.staff_id 
				and p.last_update  between ds.effective_date_from and ds.effective_date_to 
			join core.dim_date dt on dt.date_actual = p.payment_date::date;
		
		--- получаем список платежей по которым не было изминений, по полям по которым поддерживается историчность 
		create temporary table updated_payments_wo_history  on commit drop as 
		select
			p.payment_id 
		from 
			staging.payment p 
			join core.fact_payment fp 
				on p.payment_id = fp.payment_id 
				and p.last_update between fp.effective_date_from and fp.effective_date_to 
			join core.dim_date dd 
				on dd.date_dim_pk = fp.payment_date_fk 
			where 
				p.amount = fp.amount 
				and p.payment_date ::date = dd.date_actual 
				and p.rental_id = fp.rental_id ;
			
		--- проставляем новые значение полей по тем платежам по которым не нужна историчность
		update core.fact_payment fp 
		set inventory_fk = di.inventory_pk,
			staff_fk = ds.staff_pk 
		from 
			updated_payments_wo_history pwoh
			join staging.payment p 
				on p.payment_id = pwoh.payment_id
			join core.dim_inventory di 
				on p.inventory_id = di.inventory_id -- присоединяем только те кто входит в диапазон между обозгаченными датами в 
				and p.last_update  between di.effective_date_from and di.effective_date_to 
			join core.dim_staff ds
				on p.staff_id = ds.staff_id 
				and p.last_update  between ds.effective_date_from and ds.effective_date_to 
		where 
			p.payment_id = fp.payment_id 
			and p.last_update between fp.effective_date_from and fp.effective_date_to; 
		
		--- помечпем платежи по изминениям которых нужно реализрвать историчность не активными 
		update core.fact_payment fp
		set 
			is_active = false,
			effective_date_to = p.last_update 
		from staging.payment p
			left join updated_payments_wo_history pwoh 
				on p.payment_id = pwoh.payment_id
			left join new_payment_id_list np 
			    on p.payment_id = np.payment_id
		where 
			p.payment_id = fp.payment_id 
			and fp.is_active is  true
			and pwoh.payment_id is null
			and p.deleted is null
			and np.payment_id is null;
			
		
			
		--- по измененным платежам, по котрым нужна истоирчность добавляем новые актуальные строки 
		
		insert into core.fact_payment
		(
			payment_id,
			amount,
			payment_date_fk,
			inventory_fk,
			staff_fk,
			rental_id ,
			effective_date_from ,
			effective_date_to ,
			is_active 
		)
		select
			p.payment_id,
			p.amount,
			dt.date_dim_pk as payment_date_fk,
			di.inventory_pk as inventory_fk,
			ds.staff_pk as staff_fk,
			p.rental_id,
			p.last_update as effective_date_from,
			'9999-01-01'::date as effective_date_to,
			true as is_active 
			
		from
			staging.payment p
			left join new_payment_id_list np using (payment_id)
			left join updated_payments_wo_history pwoh using (payment_id)
			join core.dim_inventory di 
				on p.inventory_id = di.inventory_id -- присоединяем только те кто входит в диапазон между обозгаченными датами в 
				and p.last_update  between di.effective_date_from and di.effective_date_to 
			join core.dim_staff ds
				on p.staff_id = ds.staff_id 
				and p.last_update  between ds.effective_date_from and ds.effective_date_to 
			join core.dim_date dt on dt.date_actual = p.payment_date::date
	   where 
	   		pwoh.payment_id is null 
	   		and np.payment_id is null 
	  	    and p.deleted is null;
	end;
$procedure$
;

------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE staging.full_load()
 LANGUAGE plpgsql
AS $procedure$
    declare 
		current_update_dt timestamp = now();
	begin
		call staging.film_load(current_update_dt);
		call staging.inventory_load(current_update_dt);
		call staging.rental_load(current_update_dt);
		call staging.payment_load(current_update_dt);
		call staging.staff_load(current_update_dt);
		call staging.address_load();
		call staging.city_load();
		call staging.store_load();
		
		
		--call core.fact_delete();
		call core.load_inventory();
		call core.load_staff();
		call core.load_payment();
		call core.load_rental();
	
		call report.sales_date_calc();
		call report.sales_film_calc();
	end;
$procedure$
;

call staging.full_load();

select * from staging.payment p 
select *  from staging.last_update lu 