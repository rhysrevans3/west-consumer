import logging

from esgf_core_utils.models.kafka.consumer import KafkaConsumer

from src.message_processor import message_processor

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO
)


if __name__ == "__main__":

    consumer = KafkaConsumer(message_processor=message_processor)

    consumer.start()
