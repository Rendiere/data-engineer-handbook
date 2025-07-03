-- Load actors SCD table up until 2023


with streak_started as (
	select 
		actor_name,
		year,
		quality_class,
		is_active,
		lag(quality_class, 1) over (partition by actor_name order by year) <> quality_class 
			OR
		lag(quality_class, 1) over (partition by actor_name order by year) is null
			OR
		lag(is_active, 1) over (partition by actor_name order by year) <> is_active 
			or
		lag(is_active, 1) over (partition by actor_name order by year) is null
		as did_change
	from actors
	where year <= 2021
),streak_identified as (
	select 
		actor_name,
		quality_class,
		is_active,
		year,
		did_change,
		sum(case when did_change then 1 else 0 end) over (partition by actor_name order by year) as streak_identifier
	from streak_started
), aggregated as (
	select 
		actor_name,
		quality_class,
		is_active,
		streak_identifier,
		min(year) as start_date,
		max(year) as end_date
	from streak_identified 
	group by 1,2,3,4
)
insert into actors_history_scd (
	actor_name, 
	quality_class, 
	is_active, 
	start_date, 
	end_date,
	current_year
)
select 
	actor_name, 
	quality_class, 
	is_active, 
	start_date, 
	end_date,
	2021 as current_year
from aggregated
on conflict (actor_name, start_date) do update
set
	quality_class = excluded.quality_class,
	is_active = excluded.is_active,
	end_date = excluded.end_date
;
