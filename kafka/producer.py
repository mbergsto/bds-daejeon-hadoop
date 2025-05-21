import json
import subprocess
from collections import defaultdict
from confluent_kafka import Producer

# Set up Kafka Producer
producer = Producer({
    'bootstrap.servers': '172.21.229.182',  # Raspberry Pi 1 IP
    #'bootstrap.servers': 'localhost:9092',  # Local Kafka broker for testing
    'client.id': 'hadoop-producer'
})

# Utility function to read lines from HDFS file
def read_hdfs_file(path):
    result = subprocess.run(['hdfs', 'dfs', '-cat', path], capture_output=True, text=True)
    return result.stdout.strip().splitlines()

# Parse team stats from HDFS
team_stats = {}
for line in read_hdfs_file('/user/baseball/processed/team_stats/part-*'):
    parts = line.strip().split("\t")
    if len(parts) != 2:
        continue
    team, stats_str = parts
    stats = dict(s.split(":") for s in stats_str.split())
    team_stats[team] = {
        "wins": int(stats.get("Wins", 0)),
        "losses": int(stats.get("Losses", 0)),
        "draws": int(stats.get("Draws", 0)),
        "score_difference": int(stats.get("ScoreDiff", 0))
    }

# Parse batter stats and group by team
batters_by_team = defaultdict(list)
for line in read_hdfs_file('/user/baseball/processed/batter_stats/part-*'):
    parts = line.strip().split("\t")
    if len(parts) != 3:
        continue
    player, team, stats_str = parts
    stats = dict(s.split(":") for s in stats_str.split())
    batters_by_team[team].append({
        "player_name": player,
        "batting_average": float(stats.get("AVG", 0)),
        "on_base_percentage": float(stats.get("OBP", 0)),
        "form_score": None
    })

# Parse pitcher stats and group by team
pitchers_by_team = defaultdict(list)
for line in read_hdfs_file('/user/baseball/processed/pitcher_stats/part-*'):
    parts = line.strip().split("\t")
    if len(parts) < 6:
        continue
    player = parts[0]
    team = parts[1]
    stats = dict(s.split(":") for s in parts[2:])
    pitchers_by_team[team].append({
        "player_name": player,
        "era": float(stats.get("ERA", 0)),
        "whip": float(stats.get("WHIP", 0)),
        "k_per_9": float(stats.get("K/9", 0)),
        "bb_per_9": float(stats.get("BB/9", 0)),
        "form_score": None
    })

# Combine all stats per team and send to Kafka
for team in team_stats:
    team_data = {
        "team_name": team,
        "team_stats": team_stats[team],
        "batters": batters_by_team.get(team, []),
        "pitchers": pitchers_by_team.get(team, [])
    }
    try:
        value = json.dumps(team_data)
        producer.produce("processed_team_stats", key=team, value=value)
    except Exception as e:
        print(f"Failed to produce message for team {team}: {e}")

producer.flush()  # Ensure all messages are sent
print("All team data produced to Kafka.")
