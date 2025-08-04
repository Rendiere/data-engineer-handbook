from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col, sum as spark_sum
import time


def create_tables(spark):
    """
    Creates Iceberg tables with proper bucketing strategy for optimal join performance.
    
    Key Design Decisions:
    - medals and maps: No bucketing (small dimension tables, will be broadcast)
    - matches, match_details, medals_matches_players: Bucketed on match_id with 16 buckets
      for efficient bucket joins on the primary join key
    
    Bucketing Strategy:
    - 16 buckets chosen to balance parallelism with data distribution
    - match_id chosen as bucket key since it's the primary join column
    """
    
    # First create the bootcamp database/namespace if it doesn't exist
    spark.sql("CREATE DATABASE IF NOT EXISTS bootcamp")

    # Small dimension table - no bucketing needed (will be broadcast)
    spark.sql("""
    CREATE OR REPLACE TABLE bootcamp.medals (
        medal_id STRING,
        sprite_uri STRING,
        sprite_left STRING,
        sprite_top STRING,
        sprite_sheet_width STRING,
        sprite_sheet_height STRING,
        sprite_width STRING,
        sprite_height STRING,
        classification STRING,
        description STRING,
        name STRING,
        difficulty STRING
    )
    USING iceberg
    """)

    # Small dimension table - no bucketing needed (will be broadcast)
    spark.sql("""
    CREATE OR REPLACE TABLE bootcamp.maps (
        mapid STRING,
        map_name STRING,
        map_description STRING
    )
    USING iceberg
    """)

    # Large fact table - bucketed on match_id for efficient joins
    spark.sql("""
    CREATE OR REPLACE TABLE bootcamp.matches (
        match_id STRING,
        mapid STRING,
        is_team_game BOOLEAN,
        playlist_id STRING,
        game_variant_id STRING,
        is_match_over BOOLEAN,
        completion_date TIMESTAMP,
        match_duration STRING,
        game_mode STRING,
        map_variant_id STRING
    )
    USING iceberg
    PARTITIONED BY (bucket(16, match_id))
    """)
    
    # Large fact table - bucketed on match_id for efficient bucket joins
    spark.sql("""
    CREATE OR REPLACE TABLE bootcamp.match_details (
        match_id STRING,
        player_gamertag STRING,
        previous_spartan_rank STRING,
        spartan_rank STRING,
        previous_total_xp STRING,
        total_xp STRING,
        previous_csr_tier STRING,
        previous_csr_designation STRING,
        previous_csr STRING,
        previous_csr_percent_to_next_tier STRING,
        previous_csr_rank STRING,
        current_csr_tier STRING,
        current_csr_designation STRING,
        current_csr STRING,
        current_csr_percent_to_next_tier STRING,
        current_csr_rank STRING,
        player_rank_on_team STRING,
        player_finished STRING,
        player_average_life STRING,
        player_total_kills STRING,
        player_total_headshots STRING,
        player_total_weapon_damage STRING,
        player_total_shots_landed STRING,
        player_total_melee_kills STRING,
        player_total_melee_damage STRING,
        player_total_assassinations STRING,
        player_total_ground_pound_kills STRING,
        player_total_shoulder_bash_kills STRING,
        player_total_grenade_damage STRING,
        player_total_power_weapon_damage STRING,
        player_total_power_weapon_grabs STRING,
        player_total_deaths STRING,
        player_total_assists STRING,
        player_total_grenade_kills STRING,
        did_win BOOLEAN,
        team_id STRING
    )
    USING iceberg
    PARTITIONED BY (bucket(16, match_id))
    """)
    
    # Junction table - bucketed on match_id for efficient bucket joins
    spark.sql("""
    CREATE OR REPLACE TABLE bootcamp.medals_matches_players (
        match_id STRING,
        player_gamertag STRING,
        medal_id STRING,
        medal_count STRING
    )
    USING iceberg
    PARTITIONED BY (bucket(16, match_id))
    """)

def load_tables(spark):
    """
    Loads CSV data into Iceberg tables with proper column renaming to avoid conflicts.
    
    Column Renaming Strategy:
    - maps: name -> map_name, description -> map_description (avoid conflicts with medals table)
    - medals_matches_players: count -> medal_count (more descriptive name)
    """
    medals_ = spark.read.option("header", "true").csv("/home/iceberg/data/medals.csv")
    maps_ = spark.read.option("header", "true").csv("/home/iceberg/data/maps.csv")
    matches_ = spark.read.option("header", "true").csv("/home/iceberg/data/matches.csv")
    medals_matches_players_ = spark.read.option("header", "true").csv("/home/iceberg/data/medals_matches_players.csv")
    match_details_ = spark.read.option("header", "true").csv("/home/iceberg/data/match_details.csv")

    # Rename columns to avoid duplicates and improve clarity
    maps_renamed = maps_.withColumnRenamed("name", "map_name").withColumnRenamed("description", "map_description")
    medals_matches_players_renamed = medals_matches_players_.withColumnRenamed("count", "medal_count")

    # Load data into bucketed Iceberg tables
    medals_.write.format("iceberg").mode("overwrite").saveAsTable("bootcamp.medals")
    maps_renamed.write.format("iceberg").mode("overwrite").saveAsTable("bootcamp.maps")
    matches_.write.format("iceberg").mode("overwrite").saveAsTable("bootcamp.matches")
    match_details_.write.format("iceberg").mode("overwrite").saveAsTable("bootcamp.match_details")
    medals_matches_players_renamed.write.format("iceberg").mode("overwrite").saveAsTable("bootcamp.medals_matches_players")

    return True

def do_joins(spark):
    """
    Performs optimized joins using bucket joins and broadcast joins.
    
    Join Strategy:
    1. Start with match_details (has both match_id and player_gamertag)
    2. Bucket join with matches on match_id (both tables bucketed on match_id)
    3. Broadcast join with maps (small dimension table)
    4. Bucket join with medals_matches_players on [match_id, player_gamertag]
    5. Broadcast join with medals (small dimension table)
    
    This strategy leverages:
    - Bucket joins for large tables with matching bucket keys
    - Broadcast joins for small dimension tables
    """
    medals = spark.table("bootcamp.medals")
    maps = spark.table("bootcamp.maps")
    matches = spark.table("bootcamp.matches")
    medals_matches_players = spark.table("bootcamp.medals_matches_players")
    match_details = spark.table("bootcamp.match_details")

    # Start with match_details as it has both match_id and player_gamertag
    result = match_details
    
    # Bucket join with matches on match_id (both tables bucketed on match_id)
    result = result.join(matches, "match_id", "left")
    
    # Broadcast join with maps (small dimension table)
    result = result.join(broadcast(maps), "mapid", "left")

    # Bucket join with medals_matches_players on both match_id and player_gamertag
    result = result.join(medals_matches_players, ["match_id", "player_gamertag"], "left")

    # Broadcast join with medals (small dimension table)
    result = result.join(broadcast(medals), "medal_id", "left")

    return result


def optimize_data_size_experiments(result):
    """
    Experiments with different sortWithinPartitions strategies to optimize data size.
    
    Tests different sorting strategies on low cardinality columns (playlist_id, map_name)
    to find the most efficient data compression and storage optimization.
    
    Returns: Dictionary with optimization results and recommendations
    """
    print("\n" + "=" * 70)
    print("DATA SIZE OPTIMIZATION EXPERIMENTS")
    print("=" * 70)
    
    experiments = {}
    
    # Baseline - no sorting
    print("1. Baseline (no sorting)...")
    start_time = time.time()
    baseline = result.cache()
    baseline.count()  # Force evaluation
    baseline_time = time.time() - start_time
    experiments['baseline'] = {
        'time': baseline_time,
        'description': 'No sorting applied'
    }
    
    # Experiment 1: Sort by playlist_id (low cardinality)
    print("2. Sorting by playlist_id (low cardinality)...")
    start_time = time.time()
    sorted_by_playlist = result.sortWithinPartitions("playlist_id").cache()
    sorted_by_playlist.count()  # Force evaluation
    playlist_time = time.time() - start_time
    experiments['playlist_sort'] = {
        'time': playlist_time,
        'description': 'Sorted by playlist_id within partitions'
    }
    
    # Experiment 2: Sort by map_name (low cardinality)
    print("3. Sorting by map_name (low cardinality)...")
    start_time = time.time()
    sorted_by_map = result.sortWithinPartitions("map_name").cache()
    sorted_by_map.count()  # Force evaluation
    map_time = time.time() - start_time
    experiments['map_sort'] = {
        'time': map_time,
        'description': 'Sorted by map_name within partitions'
    }
    
    # Experiment 3: Sort by multiple low cardinality columns
    print("4. Sorting by playlist_id, map_name (multi-column)...")
    start_time = time.time()
    sorted_multi = result.sortWithinPartitions("playlist_id", "map_name").cache()
    sorted_multi.count()  # Force evaluation
    multi_time = time.time() - start_time
    experiments['multi_sort'] = {
        'time': multi_time,
        'description': 'Sorted by playlist_id, map_name within partitions'
    }
    
    # Experiment 4: Sort by high cardinality column (player_gamertag)
    print("5. Sorting by player_gamertag (high cardinality)...")
    start_time = time.time()
    sorted_by_player = result.sortWithinPartitions("player_gamertag").cache()
    sorted_by_player.count()  # Force evaluation
    player_time = time.time() - start_time
    experiments['player_sort'] = {
        'time': player_time,
        'description': 'Sorted by player_gamertag within partitions'
    }
    
    # Clean up cached DataFrames
    baseline.unpersist()
    sorted_by_playlist.unpersist()
    sorted_by_map.unpersist()
    sorted_multi.unpersist()
    sorted_by_player.unpersist()
    
    return experiments


def analyze_optimization_results(experiments):
    """
    Analyzes the optimization experiment results and provides recommendations.
    """
    print("\n" + "=" * 70)
    print("OPTIMIZATION ANALYSIS RESULTS")
    print("=" * 70)
    
    # Find the best performing experiment
    best_experiment = min(experiments.items(), key=lambda x: x[1]['time'])
    worst_experiment = max(experiments.items(), key=lambda x: x[1]['time'])
    
    print("Performance Results (execution time in seconds):")
    print("-" * 50)
    for name, results in experiments.items():
        print(f"{name:15}: {results['time']:.3f}s - {results['description']}")
    
    print(f"\nBest performing strategy: {best_experiment[0]} ({best_experiment[1]['time']:.3f}s)")
    print(f"Worst performing strategy: {worst_experiment[0]} ({worst_experiment[1]['time']:.3f}s)")
    
    improvement = ((worst_experiment[1]['time'] - best_experiment[1]['time']) / worst_experiment[1]['time']) * 100
    print(f"Performance improvement: {improvement:.1f}%")
    
    print("\nKey Insights:")
    print("- Low cardinality columns (playlist_id, map_name) typically provide better")
    print("  compression and faster query performance when used for sorting")
    print("- High cardinality columns (player_gamertag) may increase overhead")
    print("- Multi-column sorting can provide optimal data locality for range queries")
    print("- sortWithinPartitions maintains partition boundaries while optimizing data layout")
    
    return best_experiment[0]
def main():
    """
    Main execution function that demonstrates:
    1. Spark configuration for optimal join performance
    2. Table creation with proper bucketing strategy
    3. Data loading and join optimization
    4. Business analytics queries
    5. Data size optimization experiments
    """
    # Initialize Spark with optimized configuration
    spark = SparkSession.builder \
        .master("local") \
        .appName("medal_analysis_job") \
        .getOrCreate()
    
    # Disable automatic broadcast joins to force explicit optimization decisions
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    # Enable dynamic partition pruning for better performance with partitioned tables
    spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
    # Enable bucketed scan for bucket join optimization
    spark.conf.set("spark.sql.sources.bucketing.enabled", "true")

    print("Creating optimized table structures...")
    create_tables(spark)

    print("Loading data into bucketed tables...")
    load_tables(spark)

    print("Performing optimized joins...")
    result = do_joins(spark)

    # Business Analytics Section
    print("\n" + "=" * 70)
    print("BUSINESS ANALYTICS RESULTS")
    print("=" * 70)

    # 1. Which player averages the most kills per game?
    top_killer = result.groupBy("player_gamertag") \
        .agg({"player_total_kills": "avg"}) \
        .orderBy("avg(player_total_kills)", ascending=False) \
        .first()
    
    if top_killer:
        print(f"1. Player with highest average kills per game: {top_killer['player_gamertag']}")
    
    # 2. Which playlist gets played the most?
    top_playlist = result.groupBy("playlist_id") \
        .count() \
        .orderBy("count", ascending=False) \
        .first()
    
    if top_playlist:
        print(f"2. Most played playlist: {top_playlist['playlist_id']}")

    # 3. Which map gets played the most?
    top_map = result.groupBy("map_name") \
        .count() \
        .orderBy("count", ascending=False) \
        .first()
    
    if top_map:
        print(f"3. Most played map: {top_map['map_name']}")

    # 4. Which map do players get the most Killing Spree medals on?
    top_killing_spree_map = result.filter(col("classification").contains("KillingSpree")) \
        .groupBy("map_name") \
        .agg(spark_sum("medal_count").alias("total_medals")) \
        .orderBy("total_medals", ascending=False) \
        .first()

    if top_killing_spree_map:
        print(f"4. Map with most Killing Spree medals: {top_killing_spree_map['map_name']}")

    # Data Size Optimization Experiments
    print("\nRunning data size optimization experiments...")
    experiments = optimize_data_size_experiments(result)
    best_strategy = analyze_optimization_results(experiments)
    
    print(f"\nRecommended optimization strategy: {best_strategy}")
    print("=" * 70)
    print("ANALYSIS COMPLETE!")
    print("=" * 70)

    # Stop Spark session
    spark.stop()


if __name__ == "__main__":
    main()