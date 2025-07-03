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