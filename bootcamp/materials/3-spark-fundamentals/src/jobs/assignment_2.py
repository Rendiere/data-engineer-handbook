"""
# PySpark Testing Homework

- Convert 2 queries from Weeks 1-2 from PostgreSQL to SparkSQL
- Create new PySpark jobs in `src/jobs` for these queries
- Create tests in `src/tests` folder with fake input and expected output data

Submit a zip containing only the files you created.

"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *


def create_spark_session():
    """
    Create a Spark session with Iceberg support.
    """
    spark = SparkSession.builder \
        .master("local") \
        .appName("assignment-2") \
        .getOrCreate()
    return spark


def create_tables(spark):
    """
    Create the actors and actors_history_scd tables in Iceberg format.
    """
    # Create database if not exists
    spark.sql("CREATE DATABASE IF NOT EXISTS bootcamp")
    
    # Create actors table (source data)
    spark.sql("""
        CREATE OR REPLACE TABLE bootcamp.actors (
            actor_name STRING,
            year INT,
            films ARRAY<STRUCT<
                film: STRING,
                filmid: STRING,
                rating: DOUBLE,
                votes: INT
            >>,
            quality_class STRING,
            is_active BOOLEAN
        )
        USING iceberg
    """)
    
    # Create actors_history_scd table (target for SCD operations)
    spark.sql("""
        CREATE OR REPLACE TABLE bootcamp.actors_history_scd (
            actor_name STRING,
            quality_class STRING,
            is_active BOOLEAN,
            start_date INT,
            end_date INT,
            current_year INT
        )
        USING iceberg
    """)


def load_sample_data(spark):
    """
    Load sample data for testing the SCD operations.
    """
    # Sample actors data for testing
    actors_data = [
        ("Actor A", 2020, [{"film": "Movie1", "filmid": "1", "rating": 8.5, "votes": 1000}], "good", True),
        ("Actor A", 2021, [{"film": "Movie2", "filmid": "2", "rating": 9.0, "votes": 1500}], "star", True),
        ("Actor B", 2020, [{"film": "Movie3", "filmid": "3", "rating": 6.0, "votes": 500}], "average", True),
        ("Actor B", 2021, [{"film": "Movie4", "filmid": "4", "rating": 6.5, "votes": 600}], "average", False),
        ("Actor C", 2021, [{"film": "Movie5", "filmid": "5", "rating": 7.5, "votes": 800}], "good", True),
    ]
    
    schema = StructType([
        StructField("actor_name", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("films", ArrayType(StructType([
            StructField("film", StringType(), True),
            StructField("filmid", StringType(), True),
            StructField("rating", DoubleType(), True),
            StructField("votes", IntegerType(), True)
        ])), True),
        StructField("quality_class", StringType(), True),
        StructField("is_active", BooleanType(), True)
    ])
    
    actors_df = spark.createDataFrame(actors_data, schema)
    actors_df.write.format("iceberg").mode("overwrite").saveAsTable("bootcamp.actors")


def actors_scd_backfill(spark, end_year=2021):
    """
    Converted from 4. actors_history_scd_backfill.sql
    Load actors SCD table up until specified year using Slowly Changing Dimension Type 2.
    """
    
    # Clear existing data for the backfill
    spark.sql("DELETE FROM bootcamp.actors_history_scd WHERE current_year <= {}".format(end_year))
    
    # Execute the SCD backfill logic
    spark.sql(f"""
        WITH streak_started AS (
            SELECT 
                actor_name,
                year,
                quality_class,
                is_active,
                CASE 
                    WHEN LAG(quality_class, 1) OVER (PARTITION BY actor_name ORDER BY year) != quality_class 
                        OR LAG(quality_class, 1) OVER (PARTITION BY actor_name ORDER BY year) IS NULL
                        OR LAG(is_active, 1) OVER (PARTITION BY actor_name ORDER BY year) != is_active 
                        OR LAG(is_active, 1) OVER (PARTITION BY actor_name ORDER BY year) IS NULL
                    THEN TRUE
                    ELSE FALSE
                END AS did_change
            FROM bootcamp.actors
            WHERE year <= {end_year}
        ),
        streak_identified AS (
            SELECT 
                actor_name,
                quality_class,
                is_active,
                year,
                did_change,
                SUM(CASE WHEN did_change THEN 1 ELSE 0 END) 
                    OVER (PARTITION BY actor_name ORDER BY year) AS streak_identifier
            FROM streak_started
        ), 
        aggregated AS (
            SELECT 
                actor_name,
                quality_class,
                is_active,
                streak_identifier,
                MIN(year) AS start_date,
                MAX(year) AS end_date
            FROM streak_identified 
            GROUP BY actor_name, quality_class, is_active, streak_identifier
        )
        INSERT INTO bootcamp.actors_history_scd (
            actor_name, 
            quality_class, 
            is_active, 
            start_date, 
            end_date,
            current_year
        )
        SELECT 
            actor_name, 
            quality_class, 
            is_active, 
            start_date, 
            end_date,
            {end_year} AS current_year
        FROM aggregated
    """)


def actors_scd_incremental(spark, target_year=2022):
    """
    Converted from 5. actors_history_scd_incremental.sql
    Incrementally update SCD table for the target year.
    """
    
    # Check if we have current year data
    current_year_count = spark.sql(f"""
        SELECT COUNT(*) as count 
        FROM bootcamp.actors 
        WHERE year = {target_year}
    """).collect()[0]['count']
    
    if current_year_count == 0:
        print(f"No data found for year {target_year}")
        return
    
    # Clear existing data for the target year
    spark.sql(f"DELETE FROM bootcamp.actors_history_scd WHERE current_year = {target_year}")
    
    # Execute the incremental SCD logic
    spark.sql(f"""
        WITH historical_scd AS (
            SELECT      
                actor_name,
                quality_class,
                is_active,
                start_date, 
                end_date
            FROM bootcamp.actors_history_scd
            WHERE current_year = {target_year - 1}
            AND end_date < {target_year - 1}
        ), 
        last_year_scd AS (
            SELECT *
            FROM bootcamp.actors_history_scd
            WHERE current_year = {target_year - 1} 
            AND end_date = {target_year - 1}
        ),
        current_year_data AS (
            SELECT * 
            FROM bootcamp.actors
            WHERE year = {target_year}
        ), 
        unchanged_records AS (
            SELECT
                cy.actor_name,
                cy.quality_class,
                cy.is_active,
                ly.start_date,
                cy.year AS end_date
            FROM current_year_data cy
            JOIN last_year_scd ly ON cy.actor_name = ly.actor_name
            WHERE cy.quality_class = ly.quality_class
            AND cy.is_active = ly.is_active
        ), 
        changed_records_old AS (
            SELECT
                ly.actor_name,
                ly.quality_class,
                ly.is_active,
                ly.start_date,
                ly.end_date
            FROM current_year_data cy
            LEFT JOIN last_year_scd ly ON cy.actor_name = ly.actor_name
            WHERE cy.quality_class != ly.quality_class
            OR cy.is_active != ly.is_active
        ),
        changed_records_new AS (
            SELECT
                cy.actor_name,
                cy.quality_class,
                cy.is_active,
                cy.year AS start_date,
                cy.year AS end_date
            FROM current_year_data cy
            LEFT JOIN last_year_scd ly ON cy.actor_name = ly.actor_name
            WHERE cy.quality_class != ly.quality_class
            OR cy.is_active != ly.is_active
        ),
        new_records AS (
            SELECT
                cy.actor_name,
                cy.quality_class,
                cy.is_active,
                cy.year AS start_date,
                cy.year AS end_date
            FROM current_year_data cy
            LEFT JOIN last_year_scd ly ON cy.actor_name = ly.actor_name
            WHERE ly.actor_name IS NULL
        )
        INSERT INTO bootcamp.actors_history_scd (
            actor_name, 
            quality_class, 
            is_active, 
            start_date, 
            end_date,
            current_year
        )
        SELECT 
            *, 
            {target_year} AS current_year 
        FROM (
            SELECT * FROM historical_scd
            UNION ALL
            SELECT * FROM unchanged_records
            UNION ALL
            SELECT * FROM changed_records_old
            UNION ALL
            SELECT * FROM changed_records_new
            UNION ALL
            SELECT * FROM new_records
        ) combined_results
    """)


def run_scd_pipeline(spark):
    """
    Run the complete SCD pipeline: backfill then incremental.
    """
    print("Starting SCD pipeline...")
    
    # Step 1: Backfill historical data up to 2021
    print("Step 1: Running SCD backfill for 2021...")
    actors_scd_backfill(spark, 2021)
    
    # Show backfill results
    print("Backfill results:")
    spark.sql("SELECT * FROM bootcamp.actors_history_scd WHERE current_year = 2021 ORDER BY actor_name, start_date").show()
    
    # Step 2: Add 2022 data for incremental test
    print("Step 2: Adding 2022 data for incremental test...")
    additional_2022_data = [
        ("Actor A", 2022, [{"film": "Movie6", "filmid": "6", "rating": 8.0, "votes": 1200}], "good", True),  # Quality changed from star to good
        ("Actor B", 2022, [{"film": "Movie7", "filmid": "7", "rating": 7.0, "votes": 700}], "average", True),  # is_active changed from False to True
        ("Actor D", 2022, [{"film": "Movie8", "filmid": "8", "rating": 9.5, "votes": 2000}], "star", True),  # New actor
    ]
    
    schema = StructType([
        StructField("actor_name", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("films", ArrayType(StructType([
            StructField("film", StringType(), True),
            StructField("filmid", StringType(), True),
            StructField("rating", DoubleType(), True),
            StructField("votes", IntegerType(), True)
        ])), True),
        StructField("quality_class", StringType(), True),
        StructField("is_active", BooleanType(), True)
    ])
    
    df_2022 = spark.createDataFrame(additional_2022_data, schema)
    df_2022.write.format("iceberg").mode("append").saveAsTable("bootcamp.actors")
    
    # Step 3: Run incremental update for 2022
    print("Step 3: Running SCD incremental update for 2022...")
    actors_scd_incremental(spark, 2022)
    
    # Show final results
    print("Final SCD results:")
    spark.sql("SELECT * FROM bootcamp.actors_history_scd WHERE current_year = 2022 ORDER BY actor_name, start_date").show()


def main():
    """
    Main function to run the SCD pipeline.
    """
    spark = create_spark_session()
    
    try:
        # Create tables
        create_tables(spark)
        
        # Load sample data
        load_sample_data(spark)
        
        # Run SCD pipeline
        run_scd_pipeline(spark)
        
        print("SCD pipeline completed successfully!")
        
    except Exception as e:
        print(f"Error in SCD pipeline: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
