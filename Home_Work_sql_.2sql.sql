--Создать функцию (ручной вариант coalesce), принимающий 2 аргумента, и возвращающую первый непустой из этих двух аргументов.
--Функция принимает на вход любые типы данных.

drop function if exists custom_coalesce_2;
create function custom_coalesce_2(in v1 anyelement,in v2 anyelement) returns anyelement
as $$
select 
	case 
		when v1 is not null then v1
		else v2
	end;
$$ language sql 

select custom_coalesce_2(1,null),
	   custom_coalesce_2 (null,2),
	   custom_coalesce_2('1'::text,null),
	   custom_coalesce_2(0.5,null)

--Создать функцию как в первом пункте, но принимающую 3 параметра.
drop function if exists custom_coalesce_3;
create function custom_coalesce_3(in v1 anyelement,in v2 anyelement,inout v3 anyelement )
as $$
select 
	case 
		when v1 is not null then v1
		when v2 is not null then v2
		else v3
	end;
$$ language sql

select custom_coalesce_3(1,null,null),
	   custom_coalesce_3 (null,2,3),
	   custom_coalesce_3('1'::text,null,'4'::text),
	   custom_coalesce_3(0.5,null,3.2),
	   custom_coalesce_3(null,null,3.2)

--Пересоздать функцию из пункта 2, используя create or replace, добавив в ее тело комментарии, поясняющие логику выполнения.
create or replace function custom_coalesce_3(in v1 anyelement,in v2 anyelement,inout v3 anyelement )
as $$
select 
	case 
		when v1 is not null then v1 -- если 1 элемент не пуст (not null) возвращаем его 
		when v2 is not null then v2 -- если 1 элемент Пуст, проверяем 2 элемент - возвращаем его 
		else v3 -- иначе v3
	end;
$$ language sql

select custom_coalesce_3(1,null,null),
	   custom_coalesce_3 (null,2,3),
	   custom_coalesce_3('1'::text,null,'4'::text),
	   custom_coalesce_3(0.5,null,3.2),
	   custom_coalesce_3(null,null,3.2)
select custom_coalesce_3(null,null,null) --??

--Создать таблицу с одним вещественным полем. Создать процедуру, которая заполняет созданную таблицу случайными вещественными числами от 0 до 1.
--Процедура должна принимать на вход одно целое число - количество элементов, которое надо вставить в таблицу.
--Процедура должна вернуть среднее значение из всех элементов в таблице.

drop table if exists a;
create table a (a float not null);

select *
from a



drop procedure fill_a;
create procedure fill_a(in v1 integer,inout avg_a float)
as $$
insert into a
select random()
from generate_series(1,v1);
select avg(a)
from a;
$$ language sql  

call fill_a(7,null);
select avg(a) from a;


--* Создать процедуру, которая будет наполнять таблицу rental новыми записями.

--Принимает параметры:

--- nm integer - число строк, которое нужно добавить

--- dt date default null - дата rental_date, за которую нужно добавить новые записи. Если дата не задана, то находим максимальную существующую дату rental_date в таблице rental и прибавляем к ней один день.


--Компакт диски для сдачи выбираем случайным образом.













