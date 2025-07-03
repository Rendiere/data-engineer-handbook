-- Q1

CREATE TYPE film AS (
    film TEXT,
    filmid text,
    rating real,
    votes INTEGER
);

CREATE TYPE quality AS 
    ENUM ('star','good','average','bad')
;

DROP TABLE IF EXISTS actors;

CREATE TABLE actors (
	actor_name TEXT,
    year INTEGER,
    films film[],
    quality_class quality,
    is_active BOOLEAN,
    primary key (actor_name, year)
);


-- Q2
CREATE OR REPLACE PROCEDURE process_actor_data(start_year INT, end_year INT)
LANGUAGE plpgsql
AS $$
DECLARE
    current_year INT;
BEGIN
    FOR current_year IN start_year..end_year loop
	    raise notice 'Processing year: %', current_year;

        WITH previous_year AS (
			SELECT 
				actor_name,
                year,
				films,
				quality_class,
				is_active
			FROM actors
            WHERE year = current_year - 1
		),
		current_year_data AS (
			SELECT
				MAX(actor) as actor_name,
				MAX(year) as year,
				ARRAY_AGG(
					ROW(
						film, 
						filmid, 
						rating, 
						votes
					)::film
				) as films,
				CASE 
					WHEN AVG(rating) > 8 THEN 'star' 
					WHEN AVG(rating) > 7 THEN 'good' 
					WHEN AVG(rating) > 6 THEN 'average' 
					ELSE 'bad' 
				END AS quality_class
			FROM actor_films
			WHERE year = current_year
			GROUP BY actor
		),
		source_data AS (
			SELECT 
				COALESCE(py.actor_name, cy.actor_name) AS actor_name,
				current_year AS year,
				COALESCE(py.films, ARRAY[]::film[]) || COALESCE(cy.films, ARRAY[]::film[]) AS films,
				CASE
					WHEN cy.quality_class IS NOT NULL THEN cy.quality_class::quality
					ELSE py.quality_class
				END AS quality_class,
				cy.year IS NOT NULL AS is_active
			FROM previous_year py
			FULL OUTER JOIN current_year_data cy
			ON py.actor_name = cy.actor_name
		)
		INSERT INTO actors (actor_name, year, films, quality_class, is_active)
		SELECT actor_name, year, films, quality_class, is_active
		FROM source_data
		ON CONFLICT (actor_name, year) DO UPDATE
		SET
			films = EXCLUDED.films,
			quality_class = EXCLUDED.quality_class,
			is_active = EXCLUDED.is_active;
       	RAISE NOTICE 'Year % processed successfully.', current_year;
    END LOOP;
   -- Final log after completing the loop
    RAISE NOTICE 'Processing completed for years % to %.', start_year, end_year;
END;
$$;

CALL process_actor_data (1971, 2025);



-- Q3


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

-- Q4

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


-- Q5

-- DROP TYPE IF EXISTS scd_type;
-- CREATE TYPE scd_type AS (
--     quality_class quality,
--     is_active boolean,
--     start_date INTEGER,
--     end_date INTEGER
-- );


-- This should return null before we run the incremental load
select * 
from actors_history_scd 
where current_year = 2022 
order by actor_name, start_date;


WITH historical_scd AS (
    SELECT      
        actor_name,
        quality_class,
        is_active,
        start_date, 
        end_date
    FROM actors_history_scd
    WHERE 1=1
    AND current_year = 2021
    AND end_date < 2021
), last_year_scd AS (
    SELECT
       *
    FROM actors_history_scd
    WHERE 1=1
    AND current_year = 2021 
    AND end_date = 2021
),
current_year AS (
    SELECT * FROM actors
    WHERE year = 2022
), 
unchanged_records AS (
    SELECT
        cy.actor_name,
        cy.quality_class,
        cy.is_active,
        ly.start_date,
        cy.year AS end_date
    FROM current_year cy
    JOIN last_year_scd ly
    ON cy.actor_name = ly.actor_name
    WHERE cy.quality_class = ly.quality_class
    AND cy.is_active = ly.is_active
), changed_records AS (
    SELECT
        cy.actor_name,
        UNNEST(ARRAY[
            ROW(
                ly.quality_class,
                ly.is_active,
                ly.start_date,
                ly.end_date
            )::scd_type,
            ROW(
                cy.quality_class,
                cy.is_active,
                cy.year,
                cy.year
            )::scd_type
        ]) AS records
    FROM current_year cy
    LEFT JOIN last_year_scd ly
    ON cy.actor_name = ly.actor_name
    WHERE cy.quality_class <> ly.quality_class
    OR cy.is_active <> ly.is_active
),
unnested_changed_records AS (
    SELECT
        actor_name,
        (records::scd_type).quality_class AS quality_class,
        (records::scd_type).is_active AS is_active,
        (records::scd_type).start_date AS start_date,
        (records::scd_type).end_date AS end_date
    FROM changed_records
),
new_records AS (
    SELECT
        cy.actor_name,
        cy.quality_class,
        cy.is_active,
        cy.year AS start_date,
        cy.year AS end_date
    FROM current_year cy
    LEFT JOIN last_year_scd ly
    ON cy.actor_name = ly.actor_name
    WHERE ly.actor_name IS NULL
)
INSERT INTO actors_history_scd (
    actor_name, 
    quality_class, 
    is_active, 
    start_date, 
    end_date,
    current_year
)
SELECT 
    *, 
    2022 AS current_year 
FROM (
    SELECT * FROM historical_scd

    UNION ALL

    SELECT * FROM unchanged_records

    UNION ALL

    SELECT * FROM unnested_changed_records

    UNION ALL

    SELECT * FROM new_records
) a
ON CONFLICT (actor_name, start_date) DO UPDATE
SET
    quality_class = EXCLUDED.quality_class,
    is_active = EXCLUDED.is_active,
    end_date = EXCLUDED.end_date
;


-- This should return the updated records after running the incremental load
select * from actors_history_scd where current_year = 2022 order by actor_name asc, start_date desc