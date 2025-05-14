import json
import os
import logging
from datetime import datetime
from confluent_kafka import Consumer, KafkaException
import subprocess

# === Configure logging ===
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# === Kafka setup ===
bootstrap =  "172.21.229.182"  # Get Kafka broker address
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
hdfs_base = "/user/kbo/ingested"  # HDFS target directory

try:
    os.makedirs("/tmp", exist_ok=True)  # Ensure /tmp directory exists
    open(local_tmp, 'w').close()  # Clear buffer file at start

    logging.info("Starting Kafka consumer...")
    while True:
        msg = consumer.poll(timeout=1.0)  # Poll Kafka for new message
        if msg is None:
            continue  # No message received
        if msg.error():
            raise KafkaException(msg.error())  # Handle message error

        try:
            # Parse the Kafka message payload as JSON
            data = json.loads(msg.value().decode('utf-8').replace("'", '"'))
            # Log game info
            game_id = data.get("game_id", "UNKNOWN")
            logging.info(f"Processing game_id: {game_id}")

            # Append the message to the local temp file
            with open(local_tmp, 'a') as f:
                json.dump(data, f, ensure_ascii=False)
                f.write('\n')

            # Generate today's date for HDFS file path
            today = datetime.now().strftime("%Y%m%d")
            hdfs_target = f"{hdfs_base}/{today}.jl"

            # If file does not exist, create it
            if subprocess.run(["hdfs", "dfs", "-test", "-e", hdfs_target]).returncode != 0:
                subprocess.run(["hdfs", "dfs", "-touchz", hdfs_target], check=True)

            # Append temp file to HDFS file
            subprocess.run([
                "hdfs", "dfs", "-appendToFile", local_tmp, hdfs_target
            ], check=True)

            # Clear the buffer file after uploading to HDFS
            open(local_tmp, 'w').close()

            logging.info(f"Wrote to HDFS: {hdfs_target}")

        except Exception as e:
            # Handle JSON parsing or HDFS command errors
            logging.error(f"Failed to process message: {e}")

finally:
    consumer.close()  # Ensure consumer is closed properly on exit
