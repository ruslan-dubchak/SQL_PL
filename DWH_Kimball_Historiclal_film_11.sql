--- Пометим тблицу Фильм в стейжинг слое - добавим ее в отслеживание 

create or replace procedure staging.film_load()
 as $$
 	--- Добавляем переменную начала вызова функции 	
		declare 
		  current_update_dt timestamp = now();
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
	insert
	into staging.last_update
	(
	table_name,
	update_dt
	)
	values(
	'staging.film',
	current_update_dt -- время начала загрузки данных 
);
	end;
$$ language plpgsql;



-- добавим начало загрузки в деколарировани процедуры 
create or replace  procedure staging.staff_load()
as $$
declare 
	last_update_dt timestamp;
	current_update_dt timestamp = now();-- время начала загрузки 
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
insert
	into staging.last_update
	(
	table_name,
	update_dt
	)
	values(
	'staging.staff',
	current_update_dt -- время начала загрузки данных 
);
end;
$$ language plpgsql;


-- добавим начало загрузки в деколарировани процедуры 
create or replace  procedure staging.inventory_load() 
as $$
declare 
	last_update_dt timestamp;
	current_update_dt timestamp = now();-- время начала загрузки 
begin
	last_update_dt = coalesce(
						(
						select  
							max(update_dt) 
						from  
							staging.last_update
						where 
						 	table_name = 'staging.inventory'
						 ),
						'1900-01-01'::date
					 	);			 	
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
insert
	into staging.last_update
	(
	table_name,
	update_dt
	)
	values(
	'staging.inventory',
	current_update_dt-- время начала загрузки данных
);
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
		and is_active is true;
	
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
	true as is_active 
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

call full_load();

select * from staging.last_update lu ;

select * from core.dim_inventory di where di.film_id = 997

