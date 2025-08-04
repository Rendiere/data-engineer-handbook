# Test Data for Assignment 2

This directory contains test data and expected outputs for the SCD (Slowly Changing Dimension) operations.

## Input Data

### actors_input.json
Sample actors data for testing the SCD pipeline.

### actors_2022_input.json  
Additional 2022 data for testing incremental SCD operations.

## Expected Outputs

### expected_scd_backfill_2021.json
Expected SCD table state after backfill operation through 2021.

### expected_scd_incremental_2022.json
Expected SCD table state after incremental update for 2022.

## Test Scenarios

1. **SCD Backfill Test**: Load historical data and create initial SCD records
2. **SCD Incremental Test**: Add new year data and update SCD table accordingly
3. **Data Quality Tests**: Validate date ranges, data types, and business rules
