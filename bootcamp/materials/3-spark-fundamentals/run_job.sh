#!/bin/bash
# Script to run medal_analysis_job.py inside the spark-iceberg container

set -e

# Path to the job inside the container
JOB_PATH="/home/iceberg/notebooks/../src/jobs/medal_analysis_job.py"

# Run the job inside the container
# This assumes the src/jobs directory is mounted into the container (as in your docker-compose)
docker exec -it spark-iceberg python $JOB_PATH
