import json
import logging

import httpx
from confluent_kafka import Consumer, KafkaException

from settings.consumer import consumer_settings

if consumer_settings.node == "ceda":
    from ceda import ConsumerSearchClient
else:
    from globus import ConsumerSearchClient


class KafkaConsumerService:
    def __init__(self):
        self.message_processor = ConsumerSearchClient()
        self.consumer = Consumer(
            consumer_settings.kafka_config.model_dump(by_alias=True, exclude_none=True)
        )

    def commit(self, message):
        if message:
            self.consumer.commit(message=message, asynchronous=False)

    def start(self):
        self.consumer.subscribe(consumer_settings.topics)

        try:
            logging.info(
                "Kafka consumer started. Subscribed to topics: %s",
                consumer_settings.topics,
            )

            while True:
                message = self.consumer.poll(timeout=consumer_settings.timeout)
                logging.info(
                    "Kafka consuming message: %s",
                    message,
                )
                if message is None:
                    continue

                if message.error():
                    logging.error(
                        "Message error at offset %s: %s.",
                        message.offset(),
                        message.error(),
                    )
                    logging.error(
                        "Message data %s.",
                        message,
                    )
                try:
                    self.message_processor.ingest(message)

                except Exception as exc:
                    try:
                        payload = {
                            "kafka_message": message,
                            "error": exc,
                        }

                        httpx.post(
                            consumer_settings.slack_hook,
                            headers={"Content-Type": "application/json"},
                            json={"text": json.dumps(payload)},
                        )

                    except Exception as e:
                        logging.error("Failed posting to Slack: %s", e)

                else:
                    self.consumer.commit(message=message, asynchronous=False)

        except KeyboardInterrupt:
            logging.info("Kafka consumer interrupted. Exiting")

        except KafkaException as e:
            logging.error("Kafka exception: %s", e)

        finally:
            logging.info("Closing Kafka consumer")

            self.consumer.close()
