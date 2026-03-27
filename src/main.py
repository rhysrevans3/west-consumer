import logging

from esgf_core_utils.models.kafka.consumer import KafkaConsumer

from src.settings import settings

if settings.node == "ceda":
    from ceda import ConsumerSearchClient
else:
    from globus import ConsumerSearchClient


logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO
)


if __name__ == "__main__":

    consumer = KafkaConsumer(message_processor=ConsumerSearchClient())

    consumer.start()
