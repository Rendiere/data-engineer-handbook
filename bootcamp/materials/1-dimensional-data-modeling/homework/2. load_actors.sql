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

-- Check the results
-- SELECT * FROM actors ORDER BY actor_name, year;