--Вывести все фильмы без ограничения по возрасту (film.rating = ‘G’). По каждому из фильмов вывести:
-- название (film.title)
--сколько всего дисков с этим фильмом (кол-во записей в inventory) (рассчитать отдельной функцией, которая принимает на вход film_id)
-- сколько раз фильм сдавали в аренду (кол-во записей в rental) (рассчитать отдельной функцией, которая принимает на вход film_id)

drop function if exists count_film_inventory;
create function count_film_inventory(film_id int) returns int8
as $$
select count(i.inventory_id) 
from inventory i 
where i.film_id = count_film_inventory.film_id
$$ language sql;


drop function if exists count_rental_film;
create function count_rental_film(film_id int) returns int8 
as $$
select count(i.inventory_id)
from inventory i 
join rental r on i.inventory_id = r.inventory_id 
where i.film_id = count_rental_film.film_id
$$ language sql;

select f.title as film_titel,
	   count_film_inventory(f.film_id) as count_inventory_film,
	   count_rental_film (f.film_id)   as  count_rental_film    
from film f 
join inventory i on i.film_id =f.film_id 
join rental r on i.inventory_id = r.inventory_id 
where f.rating in ('G')
group by f.film_id 

--Написать функцию, которая принимает на вход два целых числа типа int и возвращает наибольшее из них.
--Написать запрос с пример использования этой функции.

drop function if exists bigger;
create function bigger (in V1 int,in V2 int , out V3 int)
as $$
select 
 case 
 	when V1>V2 then V1
 	when V1=V2 then Null
 	else V2
 end
 $$ language sql;
 
select bigger(2,1) as "2",
	   bigger(3,1) as "3",
	   bigger (2,4) as "4",
	   bigger (4,4) as "0"
	   
--Написать функцию, которая добавляет в систему информацию о новом компакт диске (добавляет новую запись в таблицу inventory).

--Принимает параметры:
-- film_id - id фильма, который находится на новом компакт диске
-- store_id - id магазина, к которому будет привязан компакт диск

--Добавить 3 новых компакт диска в систему, используя новую функцию.

select *
from inventory i 
order by last_update desc 

drop function if exists new_inventory;
create function new_inventory(
	film_id int,
	store_id int) returns int
as $$
INSERT into inventory
	(film_id ,
	store_id,
	last_update)
VALUES(
	film_id, 
	store_id, 
	now());
select 1
$$language sql;

select new_inventory(1,1);
select new_inventory(2,2);
select new_inventory(3,1);

--Написать функцию, которая принимает на вход film_id и возвращает пары значений:
- --дату
--- общую сумму платежей по данному фильму за эту дату (sum(payment.amount))

--Выводим только даты, за которые был хотя бы один платеж по выбранному фильму.
--Отсортировать результат в порядке увеличения даты.
drop function if exists amount_per_date; 
create function amount_per_date(film_id int) returns table (payment_day timestamp ,amount numeric (5,2))
as $$
select 
	date_trunc('day', p.payment_date) ,
	sum(p.amount)
from inventory i 
join rental r on r.inventory_id = i.inventory_id 
join payment p on p.rental_id = r.rental_id 
where film_id = amount_per_date.film_id and p.amount !=0
group by i.film_id ,date_trunc('day', p.payment_date) 
order by date_trunc('day', p.payment_date)
$$language sql;

select * from amount_per_date(79)
