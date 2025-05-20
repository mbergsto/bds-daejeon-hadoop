from confluent_kafka import Consumer, KafkaException
import json
import logging
import os

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Kafka config 
conf = {
    'bootstrap.servers':  "172.21.229.182",  # Get Kafka broker address on Raspberry Pi 1
    #'bootstrap.servers': 'localhost:9092',  # Local Kafka broker for testing
    'group.id': 'test-processed-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['processed_team_stats'])

# Output file
output_path = "test_output/processed_teams.jl"
os.makedirs(os.path.dirname(output_path), exist_ok=True)

try:
    with open(output_path, 'a') as f:
        logging.info("Listening to 'processed_team_stats'... Press Ctrl+C to stop.")
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            try:
                data = json.loads(msg.value().decode('utf-8'))
                json.dump(data, f, ensure_ascii=False)
                f.write('\n')
                logging.info(f"Saved: {data.get('team_name', 'UNKNOWN')}")
            except Exception as e:
                logging.error(f"Error decoding or writing message: {e}")
finally:
    consumer.close()
    logging.info("Consumer closed.")
