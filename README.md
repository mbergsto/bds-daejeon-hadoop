# BDS-DAEJEON-HADOOP

This project runs MapReduce jobs to process KBO game data (JSON Lines) using Hadoop Streaming.

---

## Setup

```bash
# Start Hadoop (if not already running)
hdfs --daemon start namenode
hdfs --daemon start datanode

# Activate environment
source .env

# Upload (or overwrite) mock data to HDFS
hdfs dfs -mkdir -p /user/baseball/raw
hdfs dfs -put -f ~/kbo-project/mock_data/mock_data.jl /user/baseball/raw/
```

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

---

## View Output

```bash
hdfs dfs -cat /user/baseball/processed/batter_stats/part-00000
```

---

## HDFS Structure

```
/user/baseball/
├── raw/          ← Input (e.g., mock_data.jl)
└── processed/    ← Output folders per job (e.g., batter_stats, pitcher_stats)
```

---

## List All Files in HDFS

```bash
hdfs dfs -ls -R /
```
