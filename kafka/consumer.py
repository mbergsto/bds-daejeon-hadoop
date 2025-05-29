import json
import os
import logging
from datetime import datetime
from confluent_kafka import Consumer, KafkaException
import subprocess
import time

os.environ["RUN_ENV"] = "prod" # Set environment variable for Hadoop jobs

TRIGGER_HADOOP_JOBS = True  # Set to True to trigger Hadoop jobs after a quiet period
TRIGGER_PRODUCER = True  # Set to True to trigger the producer script after Hadoop jobs

# === Configure logging ===
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# === Kafka setup ===
bootstrap =  "172.21.229.182"  # Get Kafka broker address on Raspberry Pi 1
#bootstrap = "localhost:9092"  # Local Kafka broker for testing
group_id = "kbo-consumer-group"  # Kafka consumer group ID
topic = "kbo_game_data"  # Kafka topic to subscribe to

# Kafka consumer configuration
consumer_conf = {
    'bootstrap.servers': bootstrap,
    'group.id': group_id,
    'auto.offset.reset': 'earliest'  # Start reading from the beginning if no offset is stored
}

# Create Kafka consumer and subscribe to topic
consumer = Consumer(consumer_conf)
consumer.subscribe([topic])

# === File/HDFS config ===
local_tmp = "/tmp/kbo_buffer.jl"  # Temporary local buffer file
hdfs_base = "/user/baseball/raw/ingested"  # HDFS target directory

has_run_hadoop = False # Flag to check if Hadoop jobs have run

try:
    os.makedirs("/tmp", exist_ok=True)  # Ensure /tmp directory exists
    open(local_tmp, 'w').close()  # Clear buffer file at start
    
    last_message_time = time.time()  # Initialize last message time
    quiet_period = 90  # Time (in seconds) to wait before triggering Hadoop jobs after last message
    has_received_message = False  # Flag to check if any message has been received
    
    logging.info("Starting Kafka consumer...")
    while True:
        msg = consumer.poll(timeout=1.0)  # Poll for new Kafka messages

        if msg is not None:
            last_message_time = time.time()  # Update last message time
            has_run_hadoop = False  # Reset flag when a message is received
            has_received_message = True  # Set flag to indicate a message has been received

            if msg.error():
                raise KafkaException(msg.error())  # Raise exception if message has error

            try:
                # Parse the Kafka message as JSON
                data = json.loads(msg.value().decode('utf-8').replace("'", '"'))
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

        # If no new messages for 'quiet_period', trigger Hadoop jobs (once per quiet period)
        if TRIGGER_HADOOP_JOBS and has_received_message and not has_run_hadoop and time.time() - last_message_time > quiet_period:
            logging.info("No new messages. Triggering Hadoop jobs...")
            try:
                # Run Hadoop job scripts for batter, pitcher, and team stats
                subprocess.run(["bash", "../jobs/batter_stats/batter_stats_run.sh"], check=True)
                subprocess.run(["bash", "../jobs/pitcher_stats/pitcher_stats_run.sh"], check=True)
                subprocess.run(["bash", "../jobs/team_stats/team_stats_run.sh"], check=True)
                
                # After Hadoop jobs, run the producer script to process the data
                if TRIGGER_PRODUCER:
                    logging.info("Triggering producer script...")
                    # Run the producer script
                    subprocess.run(["python3", "producer.py"], check=True)

                has_run_hadoop = True  # Set flag to indicate Hadoop jobs have run
                logging.info("All Hadoop jobs completed successfully.")
            except subprocess.CalledProcessError as e:
                logging.error(f"Failed to run one or more Hadoop jobs: {e}")

finally:
    logging.info("Closing consumer...")
    consumer.close()  # Ensure consumer is closed properly on exit
