import json
from confluent_kafka import Producer, Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.error import KafkaException
import logging

logger = logging.getLogger(__name__)

class KafkaRepository:
    def __init__(self, broker: str, topic: str, group_id: str = None):
        self.broker = broker
        self.topic = topic
        self.producer = self._create_producer()
        self.admin_client = self._create_admin_client()
        self.consumer = None

        if group_id:
            self.start_consumer(group_id)

    def _create_producer(self):
        """Initialize and return a Kafka producer."""
        return Producer({'bootstrap.servers': self.broker})

    def _create_admin_client(self):
        """Initialize and return a Kafka admin client."""
        return AdminClient({'bootstrap.servers': self.broker})

    def produce(self, value: dict, key: str = None):
        """
        Produce a message to the Kafka topic.
        :param key: Key for partitioning the message (optional).
        :param value: Message value to send to Kafka (dictionary expected).
        """
        try:
            serialized_value = json.dumps(value)
            self.producer.produce(
                self.topic,
                key=key.encode('utf-8') if key else None,
                value=serialized_value.encode('utf-8')
            )
            self.producer.flush()
            logger.info(f"Message produced to topic {self.topic}: {serialized_value}")
        except Exception as err:
            logger.error(f"Failed to produce message to Kafka: {err}")

    def _create_consumer(self, group_id: str):
        """
        Initialize and return a Kafka consumer.
        :param group_id: Consumer group ID for offset management.
        """
        return Consumer({
            'bootstrap.servers': self.broker,
            'group.id': group_id,
            'auto.offset.reset': 'earliest'
        })
    
    def start_topic(self):
        """
        Ensure the Kafka topic is created before producing messages.
        Logs success or failure accordingly.
        """
        if self._create_topic(self.topic):
            logger.info(f"Topic '{self.topic}' created successfully.")
        else:
            logger.error(f"Failed to create topic '{self.topic}'.")
            
    
    def _create_topic(self, topic_name=None, num_partitions=1, replication_factor=1):
        """
        Create a Kafka topic if it doesn't already exist.
        https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/_modules/confluent_kafka/admin.html#AdminClient.create_topics
        
        :param topic_name: Name of the Kafka topic to create.
        :param num_partitions: Number of partitions for the topic.
        :param replication_factor: Replication factor for the topic.
        :return: True if the topic was created or already exists, False otherwise.
        """
        try:
            new_topic = NewTopic(
                topic=topic_name, 
                num_partitions=num_partitions, 
                replication_factor=replication_factor
            )
            futures = self.admin_client.create_topics([new_topic])

            for topic, future in futures.items():
                try:
                    future.result()
                    return True
                except KafkaException as err:
                    logger.warning(f"Topic '{topic}' already exists: {err}")
                    return True # TODO not sure if that's the only error returned
                except Exception as err:
                    logger.error(f"Unexpected error creating topic '{topic}': {err}")
            return False
        except Exception as err:
            logger.error(f"Admin client failed to create topic: {err}")
            return False

    def start_consumer(self, group_id: str):
        """
        Start the Kafka consumer for the specified group ID.
        :param group_id: Consumer group ID for offset management.
        """
        if not self.consumer:
            self.consumer = self._create_consumer(group_id)
            self.consumer.subscribe([self.topic])
            logger.info(f"Kafka consumer started for topic {self.topic} and group {group_id}")

    def consume(self):
        """
        Consume messages from the Kafka topic.
        :return: A list of parsed messages.
        """
        parsed_messages = []
        if not self.consumer:
            raise RuntimeError("Consumer is not started. Call start_consumer() first.")

        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    break
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.error(f"Reached end of partition for topic {msg.topic()} partition {msg.partition()}")
                    else:
                        logger.error(f"Error while consuming: {msg.error()}")
                else:
                    raw_message = msg.value().decode('utf-8')
                    try:
                        json_message = json.loads(raw_message)
                        logger.info(f"Consumed and parsed message: {json_message}")
                        parsed_messages.append(json_message)
                    except json.JSONDecodeError:
                        logger.error(f"Failed to parse message as JSON: {raw_message}")
        except Exception as err:
            logger.error(f"Failed to consume messages: {err}")
        return parsed_messages

    def close_consumer(self):
        """Close the Kafka consumer."""
        if self.consumer:
            self.consumer.close()
            self.consumer = None
            logger.info("Kafka consumer closed")
