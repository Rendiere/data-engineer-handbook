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