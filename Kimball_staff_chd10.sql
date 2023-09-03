---Историчность 
select *
from public.staff s ;

select *
from staging.staff s ;

-- Добавим в поле last_update in staging.staff

alter table staging.staff  add column last_update timestamp; -- добавили 
delete from staging.staff ; -- очистили 
alter table staging.inventory alter column last_update set not null; --устаовили oграничения не нулевых 

-- добавим в процедуру  last_update згрузки данных 
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
	now()
);
end;
$$ language plpgsql;


-- теперь добавим колонки в dim.staff: 
 -- effective_date_from and effective_date_to 
-- is_active == это флаг , что бы отбор можно было быстрый делать

alter table core.dim_staff  add column effective_date_from timestamp default to_date('1900-01-01', 'yyyy-MM-hh') not null;--самое маое число 
alter table core.dim_staff  add column effective_date_to timestamp default to_date('9999-01-01', 'yyyy-MM-hh') not null;--самое большое 
alter table core.dim_staff  add column is_active boolean default true not null; -- флаг 

-- Надо править процедуру загрузки в core.dim_dim.staff 

create or replace  procedure core.load_staff()
as $$ 
begin 
	
/*delete from core.dim_staff  s
where s.staff_id  in (
select 
	stf.staff_id  
from 
	staging.staff  stf
where 
	stf.deleted  is not null
);*/
	-- помечаем удаленные записи: 
	update core.dim_staff d
	set 
		is_active = false,
		effective_date_to = s.deleted 
	from staging.staff s  
	where 
		s.deleted is not null
		and d.staff_id  = s.staff_id 
		and is_active is true;
	
	-- получаем список идентификаторов staff_id
	create temporary table new_staff_id_list  on commit drop as 
	select
		s.staff_id 
    from
		staging.staff s 
		left join core.dim_staff ds  using(staff_id)
	where 
			ds.staff_id  is null;
		
-- добавляем новые данные  в  dim_staff
insert into core.dim_staff 

   (
	staff_id,
	first_name,
	last_name,
	address,
	district,
	city_name,
	effective_date_from,
	effective_date_to,
	is_active
	)
select
	s.staff_id,
	s.first_name,
	s.last_name,
	a.address,
	a.district,
	c.city ,
	'1900-01-01'::date as effective_date_from,
	coalesce (s.deleted,'9999-01-01'::date) as effective_date_to,
	true as is_active 
from
	staging.staff s
	join staging.store st using (store_id)
	join staging.address a using (address_id)
	join staging.city c using (city_id)
    join new_staff_id_list sdl using (staff_id);
-- помечаем измененные данные не активными
update core.dim_staff  s 
set 
	is_active = false,
	effective_date_to = ss.last_update 
from staging.staff ss
left join new_staff_id_list sdl using (staff_id)
where 
	sdl.staff_id is null
	and ss.deleted is null 
	and s.staff_id  = ss.staff_id 
	and s.is_active is true;

-- по измененым данные  добавляем актуальные строки
insert into core.dim_staff 

   (
	staff_id,
	first_name,
	last_name,
	address,
	district,
	city_name,
	effective_date_from,
	effective_date_to,
	is_active
	)
select
	s.staff_id,
	s.first_name,
	s.last_name,
	a.address,
	a.district,
	c.city ,
	'1900-01-01'::date as effective_date_from,
	coalesce (s.deleted,'9999-01-01'::date) as effective_date_to,
	true as is_active 
from
	staging.staff s
	join staging.store st using (store_id)
	join staging.address a using (address_id)
	join staging.city c using (city_id)
	left join new_staff_id_list sdl using (staff_id)
where 
	sdl.staff_id is null
	and s.deleted  is null;
end;
$$ language plpgsql; 

-- Исправим наши процедуры загрузки слоев Факта 
create or replace procedure core.load_rental()
as $$
	begin 
		delete from core.fact_rental;
	
		insert into core.fact_rental
		(
			rental_id,
			inventory_fk,
			staff_fk,
			rental_date_fk,
			return_date_fk,
			amount,
			cnt
		)
		select
			r.rental_id,
			i.inventory_pk as inventory_fk,
			s.staff_pk as staff_fk,
			dt_rental.date_dim_pk as rental_date_fk,
			dt_return.date_dim_pk as return_date_fk,
			sum(p.amount) as amount,
			count(*) as cnt
		from
			staging.rental r
			join core.dim_inventory i 
				on r.inventory_id = i.inventory_id  -- присоединяем только те кто входит в диапазон между обозгаченными датами в 
				and r.rental_date between i.effective_date_from and i.effective_date_to 
			join core.dim_staff s
				on s.staff_id = r.staff_id 
				and r.rental_date between s.effective_date_from and s.effective_date_to 	
			join core.dim_date dt_rental on dt_rental.date_actual = r.rental_date::date
			left join staging.payment p using (rental_id)
		    left join core.dim_date dt_return on dt_return.date_actual = r.return_date::date
		group by
			r.rental_id,
			i.inventory_pk,
			s.staff_pk,
			dt_rental.date_dim_pk,
			dt_return.date_dim_pk;

	end;
$$ language plpgsql;

-- Исправим наши процедуры загрузки слоев Факта core.load_payment
create or replace procedure core.load_payment()
as $$
	begin
		delete from core.fact_payment;
		insert into core.fact_payment
		(
			payment_id,
			amount,
			payment_date_fk,
			inventory_fk,
			staff_fk
		)
		select
			p.payment_id,
			p.amount,
			dt.date_dim_pk as payment_date_fk,
			di.inventory_pk as inventory_fk,
			ds.staff_pk as staff_fk
		from
			staging.payment p
			join staging.rental r using (rental_id)
			join core.dim_inventory di 
				on r.inventory_id = di.inventory_id -- присоединяем только те кто входит в диапазон между обозгаченными датами в 
				and p.payment_date between di.effective_date_from and di.effective_date_to 
			join core.dim_staff ds
				on p.staff_id = ds.staff_id 
				and p.payment_date between ds.effective_date_from and ds.effective_date_to 
			join core.dim_date dt on dt.date_actual = p.payment_date::date;

	end;
$$ language plpgsql;

--Не забыть удалить уникальность в core.dim_staff(staff_id)
ALTER TABLE core.dim_staff DROP CONSTRAINT dim_staff_staff_id;


call full_load();

select * from core.dim_staff ds ;

select * from staging.last_update lu;

select * from staging.staff s 