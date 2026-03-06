import logging

from consumer import KafkaConsumerService

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO
)


if __name__ == "__main__":

    consumer_service = KafkaConsumerService()

    consumer_service.start()
