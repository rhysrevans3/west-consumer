import logging

from consumer import KafkaConsumerService
from globus import ConsumerSearchClient
from producer import KafkaProducer
from settings.consumer import (
    event_stream,
    globus_search,
    globus_search_client_credentials,
)
from settings.producer import error_event_stream

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO
)


if __name__ == "__main__":
    error_producer = KafkaProducer(config=error_event_stream.get("config"))

    message_processor = ConsumerSearchClient(
        globus_search_client_credentials, globus_search.get("index"), error_producer
    )

    consumer_service = KafkaConsumerService(
        event_stream.get("config"),
        event_stream.get("topics"),
        message_processor,
    )
    consumer_service.start()
    consumer_service.start()
