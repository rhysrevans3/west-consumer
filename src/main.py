import logging

from esgf_core_utils.models.kafka.consumer import KafkaConsumer

from message_processor import message_processor

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO
)


def run():
    """run consumer"""
    consumer = KafkaConsumer(message_processor=message_processor)

    consumer.start()


if __name__ == "__main__":
    run()
