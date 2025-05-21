#!/bin/bash
# === ENVIRONMENT SWITCH ===
ENV=${RUN_ENV:-local}

# === CONFIGURATION ===

if [ "$ENV" = "local" ]; then       # Path to input in HDFS, either local mock data or HDFS with consumed data
  INPUT="/user/baseball/raw/mock_data.jl" 
else
  INPUT="/user/baseball/raw/ingested"
fi   
OUTPUT=/user/baseball/processed/batter_stats  # Output path in HDFS

# === CLEAN UP OLD OUTPUT IF EXISTS ===
hdfs dfs -rm -r -f $OUTPUT

# === SET SCRIPT DIRECTORY ===
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# === RUN HADOOP STREAMING JOB ===
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -input $INPUT \
  -output $OUTPUT \
  -mapper "python3 $SCRIPT_DIR/batter_stats_mapper.py" \
  -reducer "python3 $SCRIPT_DIR/batter_stats_reducer.py" \
  -file $SCRIPT_DIR/batter_stats_mapper.py \
  -file $SCRIPT_DIR/batter_stats_reducer.py
  # -mapper "python3 batter_stats_mapper.py" \
  # -reducer "python3 batter_stats_reducer.py" \
  # -file batter_stats_mapper.py \
  # -file batter_stats_reducer.py





