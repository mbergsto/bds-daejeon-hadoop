# BDS-DAEJEON-HADOOP

This project runs MapReduce jobs to process KBO game data (JSON Lines) using Hadoop Streaming.

---

## Setup

```bash
# Start Hadoop (if not already running)
hdfs --daemon start namenode
hdfs --daemon start datanode

# Activate environment, decides if you want to use consumed data or mock data
source .env

# Run Consumer to consume kbo_game_data
hdfs dfs -mkdir -p /user/baseball/raw/ingested
cd kafka
python3 consumer.py

# Or upload (or overwrite) mock data to HDFS
hdfs dfs -mkdir -p /user/baseball/raw
hdfs dfs -put -f ~/kbo-project/mock_data/mock_data.jl /user/baseball/raw/
```

---

## View HDFS Consumed Data

The Kafka consumer writes messages to HDFS using the following structure:

```
/user/baseball/raw/ingested/
├── 20250514.jl
├── 20250515.jl
...
```

Each file contains one JSON object per line (JSON Lines format), representing a single baseball game.

To view the files in HDFS:

```bash
# List all ingested files
hdfs dfs -ls /user/baseball/raw/ingested/

# View contents of a specific day's data
hdfs dfs -cat /user/baseball/raw/ingested/20250515.jl

# Count number of games ingested on a given day
hdfs dfs -cat /user/baseball/raw/ingested/20250515.jl | wc -l
```

This data is used as input for downstream Hadoop streaming jobs like team or pitcher statistics.

---

## Run a Job (Example: Batter Stats)

```bash
cd jobs/batter_stats
chmod +x run_batter_stats.sh
bash run_batter_stats.sh
```

Each `run_*.sh`:

- Deletes old output
- Runs mapper + reducer with Hadoop Streaming
- Runs on all files in the `/user/baseball/raw/ingested/` directory

---

## View Output

After running a Hadoop streaming job, the results are stored in HDFS under a processed/ subdirectory.

For example, to view the output of the batter statistics job:

```bash
# List output files
hdfs dfs -ls /user/baseball/processed/batter_stats

# View contents of the output (typically part-00000)
hdfs dfs -cat /user/baseball/processed/batter_stats/part-00000

```

Each line represents a processed output from the reducer script

---

## HDFS Structure

```
/user/baseball/
├── raw/
│   ├── mock_data.jl              ← Local development input
│   └── ingested/                 ← Live ingested data from Kafka
│       ├── 20250514.jl
│       └── 20250515.jl
└── processed/
    ├── batter_stats/            ← Output from batter stats MapReduce job
    ├── pitcher_stats/           ← Output from pitcher stats MapReduce job
    └── team_data/               ← Output from team stats MapReduce job

```

---

## List All Files in HDFS

```bash
hdfs dfs -ls -R /
```
