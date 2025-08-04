"""
Tests for assignment_2.py SCD operations.
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import sys
import os

# Add the src directory to the path so we can import our job
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from jobs.assignment_2 import create_tables, actors_scd_backfill, actors_scd_incremental


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("test-assignment-2") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "/tmp/warehouse") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def sample_actors_data():
    """Sample actors data for testing."""
    return [
        ("Alice", 2020, [{"film": "Film1", "filmid": "1", "rating": 8.5, "votes": 1000}], "good", True),
        ("Alice", 2021, [{"film": "Film2", "filmid": "2", "rating": 9.0, "votes": 1500}], "star", True),
        ("Bob", 2020, [{"film": "Film3", "filmid": "3", "rating": 6.0, "votes": 500}], "average", True),
        ("Bob", 2021, [{"film": "Film4", "filmid": "4", "rating": 6.5, "votes": 600}], "average", False),
        ("Charlie", 2021, [{"film": "Film5", "filmid": "5", "rating": 7.5, "votes": 800}], "good", True),
    ]


@pytest.fixture
def actors_schema():
    """Schema for actors table."""
    return StructType([
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


def test_create_tables(spark):
    """Test that tables are created successfully."""
    create_tables(spark)
    
    # Check that both tables exist
    tables = spark.sql("SHOW TABLES IN bootcamp").collect()
    table_names = [row['tableName'] for row in tables]
    
    assert "actors" in table_names
    assert "actors_history_scd" in table_names


def test_scd_backfill(spark, sample_actors_data, actors_schema):
    """Test SCD backfill operation."""
    # Setup: Create tables and load test data
    create_tables(spark)
    
    actors_df = spark.createDataFrame(sample_actors_data, actors_schema)
    actors_df.write.format("iceberg").mode("overwrite").saveAsTable("bootcamp.actors")
    
    # Execute backfill
    actors_scd_backfill(spark, 2021)
    
    # Verify results
    scd_results = spark.sql("""
        SELECT actor_name, quality_class, is_active, start_date, end_date, current_year 
        FROM bootcamp.actors_history_scd 
        WHERE current_year = 2021
        ORDER BY actor_name, start_date
    """).collect()
    
    # Expected results:
    # Alice: good 2020-2020, star 2021-2021
    # Bob: average 2020-2021 (no change in quality but is_active changed)
    # Charlie: good 2021-2021
    
    assert len(scd_results) >= 3  # At least 3 SCD records should be created
    
    # Check Alice's records
    alice_records = [r for r in scd_results if r['actor_name'] == 'Alice']
    assert len(alice_records) == 2  # Should have 2 records due to quality change
    
    # Check Bob's records - should have 2 records due to is_active change
    bob_records = [r for r in scd_results if r['actor_name'] == 'Bob']
    assert len(bob_records) == 2
    
    # Check Charlie's record
    charlie_records = [r for r in scd_results if r['actor_name'] == 'Charlie']
    assert len(charlie_records) == 1


def test_scd_incremental(spark, sample_actors_data, actors_schema):
    """Test SCD incremental operation."""
    # Setup: Create tables, load test data, and run backfill
    create_tables(spark)
    
    actors_df = spark.createDataFrame(sample_actors_data, actors_schema)
    actors_df.write.format("iceberg").mode("overwrite").saveAsTable("bootcamp.actors")
    
    # Run backfill first
    actors_scd_backfill(spark, 2021)
    
    # Add 2022 data
    data_2022 = [
        ("Alice", 2022, [{"film": "Film6", "filmid": "6", "rating": 8.0, "votes": 1200}], "good", True),  # Quality changed
        ("Bob", 2022, [{"film": "Film7", "filmid": "7", "rating": 7.0, "votes": 700}], "average", True),   # is_active changed
        ("David", 2022, [{"film": "Film8", "filmid": "8", "rating": 9.5, "votes": 2000}], "star", True),  # New actor
    ]
    
    df_2022 = spark.createDataFrame(data_2022, actors_schema)
    df_2022.write.format("iceberg").mode("append").saveAsTable("bootcamp.actors")
    
    # Run incremental update
    actors_scd_incremental(spark, 2022)
    
    # Verify results
    scd_results_2022 = spark.sql("""
        SELECT actor_name, quality_class, is_active, start_date, end_date, current_year 
        FROM bootcamp.actors_history_scd 
        WHERE current_year = 2022
        ORDER BY actor_name, start_date
    """).collect()
    
    # Should have records for all historical periods plus new/changed records
    assert len(scd_results_2022) >= 5
    
    # Check that David (new actor) has a record
    david_records = [r for r in scd_results_2022 if r['actor_name'] == 'David']
    assert len(david_records) == 1
    assert david_records[0]['start_date'] == 2022
    assert david_records[0]['end_date'] == 2022


def test_data_quality_checks(spark, sample_actors_data, actors_schema):
    """Test data quality checks for SCD operations."""
    # Setup
    create_tables(spark)
    
    actors_df = spark.createDataFrame(sample_actors_data, actors_schema)
    actors_df.write.format("iceberg").mode("overwrite").saveAsTable("bootcamp.actors")
    
    # Run backfill
    actors_scd_backfill(spark, 2021)
    
    # Quality checks
    scd_results = spark.sql("""
        SELECT * FROM bootcamp.actors_history_scd 
        WHERE current_year = 2021
    """).collect()
    
    # Check that all records have valid date ranges
    for record in scd_results:
        assert record['start_date'] <= record['end_date'], f"Invalid date range for {record['actor_name']}"
        assert record['start_date'] >= 2020, f"Start date too early for {record['actor_name']}"
        assert record['end_date'] <= 2021, f"End date too late for {record['actor_name']}"
        assert record['current_year'] == 2021, f"Incorrect current_year for {record['actor_name']}"


if __name__ == "__main__":
    pytest.main([__file__])
