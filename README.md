# BDS-DAEJEON-HADOOP

This project runs MapReduce jobs to process KBO game data (JSON Lines) using Hadoop Streaming.

---

## Create .env File

Create a `.env` file in the project root directory to configure your environment. This file will be used to set up the necessary environment variables for running the project.

```bash
export RUN_ENV=prod  # local (mock) or prod data to use for hadoop jobs

DB_CONNECTION=remote  # local or remote database connection

KAFKA_BOOTSTRAP_SERVER=172.21.229.182
KAFKA_TOPIC_IN=kbo_game_data
KAFKA_TOPIC_OUT=processed_team_stats

# Remote DB
DB_REMOTE_USER=bigdata
DB_REMOTE_PASSWORD=bigdata+
DB_REMOTE_HOST=192.168.1.132
DB_REMOTE_PORT=3306
DB_REMOTE_NAME=scraping_db

# Local DB
DB_LOCAL_USER=bigdata
DB_LOCAL_PASSWORD=bigdata+
DB_LOCAL_HOST=127.0.0.1
DB_LOCAL_PORT=3307
DB_LOCAL_NAME=scraping_local
```

---

## Setup with PI 2 and PI 3

```bash

# Log into the correct user
ssh hduser@<pi3-ip>

# or
su - hduser

# On both PI 2 and PI 3, run
source ~/.zshrc

# Format and start HDFS on PI 3 (namenode)
hdfs namenode -format # Only first time to initialize!
start-dfs.sh

# Start datanode on PI 2
ssh hduser@<pi2-ip>
hdfs --daemon start datanode

# Go to the Hadoop project directory on PI 3
cd ~/bds-daejeon-hadoop

# Activate environment, decides if you want to use consumed data or mock data
source .env

# Run Consumer to consume kbo_game_data
hdfs dfs -mkdir -p /user/baseball/raw/ingested
cd kafka
python3 consumer.py

# Or upload (or overwrite) mock data to HDFS
hdfs dfs -mkdir -p /user/baseball/raw
hdfs dfs -put -f ~/path-to/mock_data/mock_data.jl /user/baseball/raw/
```

---

## Check HDFS Status in Browser

You can check the status of HDFS by accessing the Namenode web interface. Open your web browser and go to:

```
http://<pi3-ip>:9870
```

## View HDFS Consumed Data

The Kafka consumer writes messages to HDFS using the following structure:
The file name is based on the date it was ingested, formatted as `YYYYMMDD.jl`. For example, if the data was ingested on May 15, 2025, the file would be named `20250515.jl`.

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

# Delete old ingested data (if needed)
hdfs dfs -rm '/user/baseball/raw/ingested/*'
```

This data is used as input for downstream Hadoop streaming jobs like team or pitcher statistics.

---

## Run a Job (Example: Batter Stats)

```bash
cd jobs/batter_stats
chmod +x batter_stats_run.sh
bash batter_stats_run.sh
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
    └── team_stats/              ← Output from team stats MapReduce job

```

---
