#!/bin/bash

# === CONFIGURATION ===
INPUT=/user/baseball/raw/mock_data.jl        # Path to input in HDFS
OUTPUT=/user/baseball/processed/batter_stats  # Output path in HDFS

# === CLEAN UP OLD OUTPUT IF EXISTS ===
hdfs dfs -rm -r -f $OUTPUT

# === RUN HADOOP STREAMING JOB ===
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -input $INPUT \
  -output $OUTPUT \
  -mapper "python3 batter_stats_mapper.py" \
  -reducer "python3 batter_stats_reducer.py" \
  -file batter_stats_mapper.py \
  -file batter_stats_reducer.py
