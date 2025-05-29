import json
import subprocess
import logging
from collections import defaultdict
from confluent_kafka import Producer
import mariadb
from dotenv import load_dotenv
import os
from datetime import datetime
import pytz

# === Setup logging ===
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# === Load environment and determine settings ===
load_dotenv()
DB_CONNECTION = os.getenv("DB_CONNECTION")
is_local = DB_CONNECTION == "local"

KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_OUT")

# === Kafka Setup ===
producer = Producer({
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVER,
    'client.id': 'hadoop-producer'
})
logging.info(f"Kafka producer initialized for topic '{KAFKA_TOPIC}'.")

# === MariaDB Setup ===
db_config = {
    "user": os.getenv("DB_LOCAL_USER") if is_local else os.getenv("DB_REMOTE_USER"),
    "password": os.getenv("DB_LOCAL_PASSWORD") if is_local else os.getenv("DB_REMOTE_PASSWORD"),
    "host": os.getenv("DB_LOCAL_HOST") if is_local else os.getenv("DB_REMOTE_HOST"),
    "port": int(os.getenv("DB_LOCAL_PORT") if is_local else os.getenv("DB_REMOTE_PORT")),
    "database": os.getenv("DB_LOCAL_NAME") if is_local else os.getenv("DB_REMOTE_NAME")
}

try:
    conn = mariadb.connect(**db_config)
    cursor = conn.cursor()
    logging.info("Connected to MariaDB.")
except mariadb.Error as e:
    logging.error(f"Failed to connect to MariaDB: {e}")
    raise

def upsert_team_data(team_name, json_data):
    korea_time = datetime.now(pytz.timezone("Asia/Seoul")).replace(tzinfo=None)
    query = """
        INSERT INTO processed_team_stats (team_name, snapshot, updated_at)
        VALUES (?, ?, ?)
        ON DUPLICATE KEY UPDATE snapshot = VALUES(snapshot)
        , updated_at = VALUES(updated_at)
    """
    cursor.execute(query, (team_name, json_data, korea_time))

def read_hdfs_file(path):
    result = subprocess.run(['hdfs', 'dfs', '-cat', path], capture_output=True, text=True)
    return result.stdout.strip().splitlines()

def calculate_team_form_score(batters, pitchers):
    total_batter_form = sum(batter['form_score'] for batter in batters)
    total_pitcher_form = sum(pitcher['form_score'] for pitcher in pitchers)
    return round((total_batter_form + total_pitcher_form) / (len(batters) + len(pitchers)), 2) if (len(batters) + len(pitchers)) > 0 else 0.0

# === Read and Process Data ===
logging.info("Reading processed stats from HDFS...")

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
        "form_score":  float(stats.get("FormScore", 0))
    })

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
        "form_score":  float(stats.get("FormScore", 0))
    })

logging.info("Publishing team data to Kafka and MariaDB...")
for team in team_stats:
    team_form_score = calculate_team_form_score(
        batters_by_team.get(team, []),
        pitchers_by_team.get(team, [])
    )
    team_data = {
        "team_name": team,
        "team_form_score": team_form_score,
        "team_stats": team_stats[team],
        "batters": batters_by_team.get(team, []),
        "pitchers": pitchers_by_team.get(team, []),
    }
    try:
        value = json.dumps(team_data)
        producer.produce(KAFKA_TOPIC, key=team, value=value)
        upsert_team_data(team, value)
        logging.info(f"Published data for team '{team}'.")
    except Exception as e:
        logging.error(f"Failed to process team {team}: {e}")

producer.flush()
conn.commit()
cursor.close()
conn.close()
logging.info("Producer finished. All data committed and connections closed.")
