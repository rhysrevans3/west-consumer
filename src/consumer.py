import logging

from confluent_kafka import Consumer, KafkaException
from esgf_playground_utils.models.kafka import KafkaEvent

from settings import consumer, event_stream

if consumer == "ceda":
    from ceda import ConsumerSearchClient
else:
    from globus import ConsumerSearchClient


class KafkaConsumerService:
    def __init__(self):
        self.message_processor = ConsumerSearchClient()
        self.consumer = Consumer(event_stream.get("config"))

    def start(self):
        self.consumer.subscribe(event_stream.get("topics"))

        try:
            logging.info(
                "Kafka consumer started. Subscribed to topics: %s",
                event_stream.get("topics"),
            )

            while True:
                message = self.consumer.poll(
                    timeout_ms=event_stream.get("timeout_ms", 5000)
                )
                logging.info(
                    "Kafka consuming message: %s",
                    message,
                )

                if message is None:
                    continue

                self.message_processor.ingest(message)

                self.consumer.commit(message=message, asynchronous=False)

        except KeyboardInterrupt:
            logging.info("Kafka consumer interrupted. Exiting...")

        except KafkaException as e:
            logging.error("Kafka exception: %s", e)

        finally:
            logging.info("Closing Kafka consumer...")

            self.consumer.close()
