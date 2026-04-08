import logging
import os

from esgf_core_utils.models.kafka.consumer import KafkaConsumer

if os.environ.get("NODE") == "ceda":
    from ceda import ConsumerSearchClient
else:
    from globus import ConsumerSearchClient


logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO
)


if __name__ == "__main__":

    consumer = KafkaConsumer(message_processor=ConsumerSearchClient())

    consumer.start()
