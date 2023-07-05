--Создать функцию на PL/pgSQL, которая принимает на вход целое число и возвращает это число умноженное на 5.

create or replace function mult_5 (in i integer) returns integer
as $$
begin 
	i:=i*5;
	return i;	
end;
$$ language plpgsql;

select mult_5(5)


--Создать функцию, которая возвращает текст 'Вы проснулись до 5 утра', если в момент вызова функции еще нет 5 утра. 
--И текст 'Вы проснулись после 5 утра', если уже 5 утра или позднее.

create or replace  function cuurent_time_morning () returns  varchar 
as $$
begin
	if current_time <='05:00:00' then return 'Вы проснулись до 5 утра';
	else return 'Вы проснулись после 5 утра';
	end if;
end;
$$ language plpgsql;

select cuurent_time_morning ()

--Создать функцию, которая на вход принимает число и в консоль выводит названия всех фильмов, у которых rental_duration равен заданному числу.
--Функция возвращает 1.

create or replace function title_of_duration (in nm_duration int ) returns int
as $$
declare 
i film%rowtype;
begin 
	for i in select *  from film f where f.rental_duration = title_of_duration.nm_duration
	loop 
    raise notice 'film whit title "%" ', i.title;
    end loop;
return 1;
end ;
$$ language plpgsql;

select title_of_duration(5)

--Создать функцию, которая на вход принимает строку и в консоль выводит названия всех фильмов, 
--у которых заданная строка является частью названия фильма. Функция возвращает 1.


drop function title_of_texttitle (varchar);
create or replace function title_of_texttitle (in txt_title varchar  ) returns int
as $$
declare 
i film%rowtype;
input_text varchar  := '%'||title_of_texttitle.txt_title||'%';
begin 
	for i in select *  from film f where f.title ilike input_text
	loop 
    raise notice 'film whit title "%" ', i.title;
    end loop;
return 1;
end ;
$$ language plpgsql;

select title_of_texttitle ('Tur')



