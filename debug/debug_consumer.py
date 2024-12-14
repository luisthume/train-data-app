from confluent_kafka import Consumer, KafkaError
import os

KAFKA_BROKER = os.environ["KAFKA_BROKER"]
TOPIC_NAME = os.environ["TOPIC_NAME"]
GROUP_ID = os.environ["GROUP_ID"]

def run_consumer():
    """Run a Kafka consumer to debug and log messages from a topic."""
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest'
    })

    consumer.subscribe([TOPIC_NAME])
    print(f"Debug consumer listening to topic '{TOPIC_NAME}'...")

    try:
        while True:
            msg = consumer.poll(1.0)  # Poll for messages with a timeout of 1 second
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"Reached end of partition for topic {msg.topic()}")
                else:
                    print(f"Consumer error: {msg.error()}")
            else:
                print(f"Consumed message: {msg.value().decode('utf-8')}")
    finally:
        consumer.close()
        print("Consumer closed.")

if __name__ == "__main__":
    run_consumer()
