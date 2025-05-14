# === CONFIGURATION ===
INPUT=/user/baseball/raw/mock_data.jl      # HDFS input path
OUTPUT=/user/baseball/processed/mock_data  # HDFS output path

# === CLEAN UP OLD OUTPUT IF EXISTS ===
hdfs dfs -rm -r -f $OUTPUT

# === RUN HADOOP STREAMING JOB ===
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -input $INPUT \
  -output $OUTPUT \
  -mapper "python3 team_stats_mapper.py" \
  -reducer "python3 team_stats_reducer.py" \
  -file team_stats_mapper.py \
  -file team_stats_reducer.py
