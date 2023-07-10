--Сделать функцию, которая на вход принимает фамилию актера.
--В функции отбираются все фильмы, в которых снимался актер с указанной фамилией. Отобранные фильмы группируются по полю film.rating.
--Для каждого рейтинга отобразить поля:
-- Сколько всего отобранных фильмов с таким рейтингом.
-- Какая средняя выручка на один фильм.
 --Сколько дисков в среднем на один фильм.

drop function if exists rating_details_by_lastName ;
create or replace  function rating_details_by_lastName (in actor_last_name varchar) returns table 
	(film_rating varchar , 
	total_count_film integer,
	avg_amount_per_film float,
	avg_count_disk_per_film float
	)
as $$
	begin
		
		--Получение рейтинга и индикатора фильма: 
		create temporary table film_rating_by_lastname on commit  drop as 	
		select 
			distinct (f.film_id) as film_id,
			f.rating
		from film f
		join film_actor fa using (film_id)
		join actor a using (actor_id)
		where 
			a.last_name = rating_details_by_lastName.actor_last_name
		group by 
			f.film_id;
		
		--Получение количества фильмов по рейтингу 
		create temporary table total_count_film on commit  drop as 
		select 
			   frl.rating,
			   count(*) as total_count_film
		from 
			  film_rating_by_lastname frl
		group by frl.rating; 
	
		--Получение стоимости фильмов 
		create temporary table total_amount_per_film on commit  drop as  
		select  i.film_id,
				sum(p.amount) as total_amount_per_film
		from rental r  
		join inventory i using(inventory_id)
		join payment p using (rental_id) 
		group by i.film_id;

		--Получение количесвта фильмов
		create temporary table total_disc_per_film on commit  drop as
		select
			i.film_id,
			count(*) as total_disc_per_film 
		from inventory i 
		group by i.film_id;
	
	
	-- получение промежуточных результатов для рейтинга, количества фильмов и выручки по рейтингу 
	create temporary table tmp_total_table on commit  drop as
	select 
	      frl.rating,
	      sum (tdp.total_disc_per_film) as tmp_total_disc,
	      sum(tpr.total_amount_per_film) as tmp_total_amount
	from film_rating_by_lastname  frl
	join total_amount_per_film tpr using (film_id)
	join total_disc_per_film   tdp using (film_id)
	group by frl.rating;
	
 	   	return query
	   	select
	   		tmp.rating::varchar,
	   		tcf.total_count_film::integer,
	   		cast ((tmp.tmp_total_amount / tcf.total_count_film) as float) as  avg_amount_per_film,
	   		cast ((tmp.tmp_total_disc / tcf.total_count_film) as float) as   avg_count_disk_per_film
	   	from tmp_total_table tmp
	   	join total_count_film tcf on tcf.rating = tmp.rating;
	   return;

	end;
$$language plpgsql;


select *
from  rating_details_by_lastName ('Wahlberg')