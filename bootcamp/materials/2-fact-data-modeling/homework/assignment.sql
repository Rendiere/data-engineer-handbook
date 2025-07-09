
-- Q1: A query to deduplicate game_details from Day 1 so there's no duplicates

-- /*
with deduped as (
    SELECT 
    g.game_date_est,
    g.season,
    g.home_team_id,
    g.visitor_team_id,
    gd.*,
    ROW_NUMBER() OVER (PARTITION BY game_id, team_id, player_id) as rn
    FROM game_details gd
    LEFT JOIN games g on gd.game_id = g.game_id

)
SELECT * FROM deduped
WHERE rn = 1;
-- */


-- Q2: A DDL for an user_devices_cumulated table
-- /*
DROP TABLE IF EXISTS user_devices_cumulated;
CREATE TABLE user_devices_cumulated (
    user_id TEXT NOT NULL,
    browser_type VARCHAR NOT NULL,
    device_activity_datelist DATE[] NOT NULL,
    date DATE NOT NULL,
    PRIMARY KEY (user_id, browser_type, date)
);
-- */

-- Q3: A query to populate user_devices_cumulated with data from events
CREATE OR REPLACE PROCEDURE populate_user_devices_cumulated(
    start_date DATE,
    end_date DATE
) AS $$
DECLARE
    processing_date DATE;
    yesterday_date DATE;
BEGIN
    processing_date := start_date;
    
    -- Loop through each date in the range
    WHILE processing_date <= end_date LOOP
        yesterday_date := processing_date - INTERVAL '1 day';
        
        -- Insert data for the current date
        INSERT INTO user_devices_cumulated
        WITH yesterday AS (
            SELECT *
            FROM user_devices_cumulated
            WHERE date = yesterday_date
        ), 
        today AS (
            SELECT e.user_id::TEXT,
                   d.browser_type,
                   DATE_TRUNC('day', e.event_time::timestamp) AS today_date
            FROM events e
            LEFT JOIN devices d ON e.device_id = d.device_id
            WHERE DATE_TRUNC('day', e.event_time::timestamp) = processing_date
              AND e.user_id IS NOT NULL
              AND e.device_id IS NOT NULL
            GROUP BY e.user_id, d.browser_type, DATE_TRUNC('day', e.event_time::timestamp)
        )
        SELECT
               COALESCE(t.user_id, y.user_id) AS user_id,
               COALESCE(t.browser_type, y.browser_type) AS browser_type,
               COALESCE(y.device_activity_datelist, ARRAY[]::DATE[]) ||
                   CASE WHEN t.user_id IS NOT NULL THEN
                       ARRAY[t.today_date]
                   ELSE
                       ARRAY[]::DATE[]
                   END AS device_activity_datelist,
                COALESCE(t.today_date, y.date + INTERVAL '1 day') AS date
        FROM yesterday y
        FULL OUTER JOIN today t 
            ON t.user_id = y.user_id 
            AND t.browser_type = y.browser_type;
        -- Move to next date
        processing_date := processing_date + INTERVAL '1 day';
        
        -- Optional: Print progress
        RAISE NOTICE 'Processed date: %', processing_date - INTERVAL '1 day';
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Call the function to populate 30 days starting from 2023-01-01
CALL populate_user_devices_cumulated('2023-01-01'::DATE, '2023-01-31'::DATE);

SELECT * FROM user_devices_cumulated
WHERE date = DATE('2023-02-01')
ORDER BY user_id, browser_type;


-- Q4: Convert device_activity_datelist to a integer list of bits
WITH starter AS (
    SELECT 
        udc.device_activity_datelist @> ARRAY [DATE(d.series_date)]   AS is_active,
        d.series_date::DATE AS series_date,
        udc.date,
        EXTRACT(DAY FROM DATE('2023-01-31') - d.series_date) AS days_since,
        udc.user_id,
        udc.device_activity_datelist
    FROM user_devices_cumulated udc
    CROSS JOIN
    (
        SELECT 
        generate_series('2023-01-01', '2023-01-31', INTERVAL '1 day') AS series_date
    ) as d
    WHERE udc.date = DATE('2023-01-31')
),
bits AS (
    SELECT
        user_id, 
        device_activity_datelist,
        STRING_AGG(
            CASE 
                WHEN is_active THEN '1' 
                ELSE '0' 
            END, 
            '' 
            ORDER BY series_date
        )::bit(32) AS datelist_int,
        DATE('2023-01-31') as date
    FROM starter
    GROUP BY user_id, device_activity_datelist
)
select * from bits where user_id = '137925124111668560'


-- Q5: hosts_cumulated table DDL
DROP TABLE IF EXISTS hosts_cumulated;
CREATE TABLE hosts_cumulated (
    host TEXT NOT NULL,
    host_activity_datelist DATE[] NOT NULL,
    date DATE NOT NULL,
    PRIMARY KEY (host, date)
);


--  Q6: Populate hosts_cumulated with data from events
CREATE OR REPLACE PROCEDURE populate_hosts_cumulated(
    start_date DATE,
    end_date DATE
) AS $$
DECLARE
    processing_date DATE;
    yesterday_date DATE;
BEGIN
    processing_date := start_date;
    
    -- Loop through each date in the range
    WHILE processing_date <= end_date LOOP
        yesterday_date := processing_date - INTERVAL '1 day';
        
        -- Insert data for the current date
        INSERT INTO hosts_cumulated
        WITH yesterday AS (
            SELECT * FROM hosts_cumulated
            WHERE date = yesterday_date
        ), 
        today AS (
            SELECT
                e.host as host,
                DATE_TRUNC('day', e.event_time::timestamp) as today_date,
                count(1) as num_events
            FROM events e
            WHERE DATE_TRUNC('day', e.event_time::timestamp) = processing_date
              AND e.host IS NOT NULL
            GROUP BY e.host, DATE_TRUNC('day', e.event_time::timestamp)
        )
        SELECT 
            COALESCE(t.host, y.host) as host,
            COALESCE(y.host_activity_datelist, ARRAY[]::DATE[]) ||
                CASE 
                    WHEN t.host IS NOT NULL THEN ARRAY[t.today_date]
                    ELSE ARRAY[]::DATE[]
                END AS host_activity_datelist,
            COALESCE(t.today_date, y.date + INTERVAL '1 day') as date
        FROM yesterday y
        FULL OUTER JOIN today t 
            ON t.host = y.host;
        
        -- Move to next date
        processing_date := processing_date + INTERVAL '1 day';
        
        -- Print progress
        RAISE NOTICE 'Processed date: %', processing_date - INTERVAL '1 day';
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Call the procedure to populate
CALL populate_hosts_cumulated('2023-01-01'::DATE, '2023-01-31'::DATE);


-- Q8: fact table host_activity_reduced DDL
DROP TABLE IF EXISTS host_activity_reduced;
CREATE TABLE host_activity_reduced
(
    host TEXT NOT NULL,
    month DATE NOT NULL,
    hit_array INT[] NOT NULL,
    unique_visitors_array INT[] NOT NULL,
    PRIMARY KEY (host, month)
);



-- Q9: Populate host_activity_reduced with data from 
CREATE OR REPLACE PROCEDURE populate_host_activity_reduced(
    start_date DATE,
    end_date DATE
) AS $$
DECLARE
    processing_date DATE;
    yesterday_date DATE;
BEGIN
    processing_date := start_date;
    
    -- Loop through each date in the range
    WHILE processing_date <= end_date LOOP
        yesterday_date := processing_date - INTERVAL '1 day';
        
        -- Insert data for the current date
        INSERT INTO host_activity_reduced (host, month, hit_array, unique_visitors_array)
        WITH yesterday AS (
            SELECT * FROM host_activity_reduced
            WHERE month = DATE_TRUNC('month', yesterday_date)
        ), 
        today AS (
            SELECT
                host,
                DATE_TRUNC('day', event_time::timestamp) AS date,
                DATE_TRUNC('month', event_time::timestamp) AS month,
                ARRAY[COUNT(1)] as hit_array,
                ARRAY[COUNT(DISTINCT user_id)] as unique_visitors_array
            FROM events
            WHERE 1=1
                AND DATE(DATE_TRUNC('day', event_time::timestamp)) = processing_date
                AND host IS NOT NULL
            GROUP BY 
                host, 
                DATE_TRUNC('day', event_time::timestamp), 
                DATE_TRUNC('month', event_time::timestamp)
        )
        SELECT
            COALESCE(t.host, y.host) AS host,
            COALESCE(t.month, DATE_TRUNC('month', y.month + INTERVAL '1 day')) AS month,
            COALESCE(y.hit_array, ARRAY[]::INT[]) || 
                CASE 
                    WHEN t.host IS NOT NULL THEN t.hit_array
                    ELSE ARRAY[]::INT[]
                END AS hit_array,
            COALESCE(y.unique_visitors_array, ARRAY[]::INT[]) ||
                CASE 
                    WHEN t.host IS NOT NULL THEN t.unique_visitors_array
                    ELSE ARRAY[]::INT[]
                END AS unique_visitors_array
        FROM yesterday y
        FULL OUTER JOIN today t 
            ON t.host = y.host
        ON CONFLICT (host, month)
        DO UPDATE SET
            hit_array = EXCLUDED.hit_array,
            unique_visitors_array = EXCLUDED.unique_visitors_array;
        
        -- Move to next date
        processing_date := processing_date + INTERVAL '1 day';
        
        -- Print progress
        RAISE NOTICE 'Processed date: %', processing_date - INTERVAL '1 day';
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Example: Call the procedure to populate for January 2023
CALL populate_host_activity_reduced('2023-01-01'::DATE, '2023-01-31'::DATE);