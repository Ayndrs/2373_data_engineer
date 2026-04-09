"""
Kafka Batch Consumer - Ingest to Landing Zone

Consumes messages from Kafka for a time window and writes to landing zone as JSON.

Pattern: Kafka Topic -> (This Script) -> ./data/landing/{topic}_{timestamp}.json

python3 jobs/ingest_kafka_to_landing.py --topic user_events --batch_duration_sec 5 --output_path ./data/landing
python3 jobs/ingest_kafka_to_landing.py --topic transaction_events --batch_duration_sec 5 --output_path ./data/landing

"""
from kafka import KafkaConsumer
import json
import time
import os
import argparse


def consume_batch(topic: str, batch_duration_sec: int, output_path: str) -> int:
    """
    Consume from Kafka for specified duration and write to landing zone.
    
    Args:
        topic: Kafka topic to consume from
        batch_duration_sec: How long to consume before writing
        output_path: Directory to write output JSON files
        
    Returns:
        Number of messages consumed
    """
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )
    except Exception:
        print("Running on localhost:9094")
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )

    os.makedirs(output_path, exist_ok=True)

    messages = []
    message_count = 0
    end_time = time.time() + batch_duration_sec

    try:
        while time.time() < end_time:
            records = consumer.poll(timeout_ms=1000)
            for _, batch in records.items():
                for record in batch:
                    messages.append(record.value)
                    message_count += 1
    finally:
        try:
            consumer.close()
        except Exception:
            pass

    timestamp = int(time.time())
    file_path = os.path.join(output_path, f"{topic}_{timestamp}.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(messages, f)

    return message_count


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Batch Consumer - Ingest to Landing Zone")
    parser.add_argument("--topic", required=True, type=str)
    parser.add_argument("--batch_duration_sec", required=False, type=int, default=int(os.getenv("BATCH_DURATION_SEC", "10")))
    parser.add_argument("--output_path", required=False, type=str, default=os.getenv("LANDING_ZONE_PATH", "./data/landing"))
    args = parser.parse_args()

    count = consume_batch(args.topic, args.batch_duration_sec, args.output_path)
    print(count)