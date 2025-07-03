
drop table if exists actors_history_scd;

CREATE TABLE actors_history_scd (
	actor_name text,
	quality_class quality,
	is_active boolean,
	start_date INTEGER,
	end_date INTEGER,
	current_year INTEGER,
	primary key (actor_name, start_date)
)