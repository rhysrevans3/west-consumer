import logging

from confluent_kafka import Consumer, KafkaException
from esgf_playground_utils.models.kafka import KafkaEvent
from pydantic_core import ValidationError

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
        self.consumer.subscribe(event_stream.topics)
        try:
            logging.info(
                "Kafka consumer started. Subscribed to topics: %s", event_stream.topics
            )
            while True:
                msg = self.consumer.poll(
                    timeout_ms=event_stream.get("timeout_ms", 5000)
                )
                if msg is None:
                    continue

                if msg.error():
                    logging.error(
                        "Message error at offset %s: %s.", msg.offset(), msg.error()
                    )

                data = KafkaEvent.model_validate(msg.value())

                self.message_processor.ingest(data)

                self.consumer.commit(message=msg, asynchronous=False)

        except KeyboardInterrupt:
            logging.info("Kafka consumer interrupted. Exiting...")

        except KafkaException as e:
            logging.error("Kafka exception: %s", e)

        except ValidationError as e:
            logging.error("Malformed Kafka event: %s", e)

        finally:
            logging.info("Closing Kafka consumer...")
            self.consumer.close()
