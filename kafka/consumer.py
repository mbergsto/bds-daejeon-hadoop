import json
import os
import logging
from datetime import datetime
from confluent_kafka import Consumer, KafkaException
import subprocess
import time
from dotenv import load_dotenv
import os

load_dotenv()

os.environ["RUN_ENV"] = "prod" # Set environment variable for Hadoop jobs

TRIGGER_HADOOP_JOBS = True  # Set to True to trigger Hadoop jobs after a quiet period
TRIGGER_PRODUCER = True  # Set to True to trigger the producer script after Hadoop jobs

# === Configure logging ===
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# === Kafka setup ===
group_id = "kbo-consumer-group"  # Kafka consumer group ID
KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER") # Kafka bootstrap server from environment variable
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_IN", default="kbo_game_data") # Kafka topic to consume from

logging.info(f"Kafka consumer initialized for topic '{KAFKA_TOPIC}' with group ID '{group_id}'.")
logging.info(f"Kafka bootstrap server: {KAFKA_BOOTSTRAP_SERVER}")

# Kafka consumer configuration
consumer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVER,
    'group.id': group_id,
    'auto.offset.reset': 'earliest'  # Start reading from the beginning if no offset is stored
}

# Create Kafka consumer and subscribe to topic
consumer = Consumer(consumer_conf)
consumer.subscribe([KAFKA_TOPIC])

# === File/HDFS config ===
local_tmp = "/tmp/kbo_buffer.jl"  # Temporary local buffer file
hdfs_base = "/user/baseball/raw/ingested"  # HDFS target directory

def run_hadoop_and_producer():
    try:
        subprocess.run(["bash", "../jobs/batter_stats/batter_stats_run.sh"], check=True)
        subprocess.run(["bash", "../jobs/pitcher_stats/pitcher_stats_run.sh"], check=True)
        subprocess.run(["bash", "../jobs/team_stats/team_stats_run.sh"], check=True)

        if TRIGGER_PRODUCER:
            logging.info("Triggering producer script...")
            subprocess.run(["python3", "producer.py"], check=True)

        logging.info("All Hadoop jobs completed successfully.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to run one or more Hadoop jobs: {e}")


try:
    os.makedirs("/tmp", exist_ok=True)  # Ensure /tmp directory exists
    open(local_tmp, 'w').close()  # Clear buffer file at start
    
    logging.info("Starting Kafka consumer...")
    while True:
        msg = consumer.poll(timeout=1.0)  # Poll for new Kafka messages

        if msg is not None:

            if msg.error():
                raise KafkaException(msg.error())  # Raise exception if message has error

            try:
                # Parse the Kafka message as JSON
                data = json.loads(msg.value().decode('utf-8').replace("'", '"'))
                
                if data.get("type") == "scrape_end":
                    logging.info("Received scrape_end message - triggering Hadoop jobs...")
                    if TRIGGER_HADOOP_JOBS:
                        run_hadoop_and_producer()
                else:
                    game_id = data.get("game_id", "UNKNOWN")
                    logging.info(f"Processing game_id: {game_id}")

                    # Append the message to the local buffer file
                    with open(local_tmp, 'a') as f:
                        json.dump(data, f, ensure_ascii=False)
                        f.write('\n')

                    # Prepare HDFS target path based on today's date
                    today = datetime.now().strftime("%Y%m%d")
                    hdfs_target = f"{hdfs_base}/{today}.jl"

                    # Create the HDFS file if it does not exist
                    if subprocess.run(["hdfs", "dfs", "-test", "-e", hdfs_target]).returncode != 0:
                        subprocess.run(["hdfs", "dfs", "-touchz", hdfs_target], check=True)

                    # Append the local buffer file to the HDFS file
                    subprocess.run(["hdfs", "dfs", "-appendToFile", local_tmp, hdfs_target], check=True)
                    open(local_tmp, 'w').close()  # Clear the local buffer file

                    logging.info(f"Wrote to HDFS: {hdfs_target}")

            except Exception as e:
                logging.error(f"Failed to process message: {e}")

finally:
    logging.info("Closing consumer...")
    consumer.close()  # Ensure consumer is closed properly on exit
    

