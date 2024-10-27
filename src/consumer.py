import logging
import json
from confluent_kafka import Consumer, TopicPartition, KafkaException


class KafkaConsumerService:
    def __init__(self, kafka_config, topics, message_processor):
        self.kafka_config = kafka_config
        self.topics = topics
        self.message_processor = message_processor
        self.consumer = Consumer(self.kafka_config)

    def process_messages(self, messages):
        messages_data = []
        for msg in messages:
            if msg.error():
                logging.error(f"Message error at offset {msg.offset()}: {msg.error()}.")
                return False
            try:
                data = json.loads(msg.value())
                messages_data.append(data)
            except json.JSONDecodeError as e:
                logging.error(f"Data deserialization error at offset {msg.offset()}: {e}.")
                return False
        return messages_data

    def start(self):
        self.consumer.subscribe(self.topics)
        try:
            logging.info(f"Kafka consumer started. Subscribed to topics: {self.topics}")
            while True:
                messages = self.consumer.consume(num_messages=50, timeout=5.0)
                if not messages:
                    continue

                logging.info(f"Consumed {len(messages)} messages")
                first_msg = messages[0]
                self.seek_partition = TopicPartition(first_msg.topic(), first_msg.partition(), first_msg.offset())

                messages_data = self.process_messages(messages)
                if not messages_data:
                    self.consumer.seek(self.seek_partition)
                    continue

                if not self.message_processor.ingest(messages_data):
                    self.consumer.seek(self.seek_partition)
                    continue

                self.consumer.commit(message=messages[-1], asynchronous=False)

        except KeyboardInterrupt:
            logging.info("Kafka consumer interrupted. Exiting...")
        except KafkaException as e:
            logging.error(f"Kafka exception: {e}")
        finally:
            logging.info("Closing Kafka consumer...")
            self.consumer.close()
