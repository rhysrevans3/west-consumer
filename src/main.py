import logging

from consumer import KafkaConsumerService
from settings import consumer, event_stream

if consumer == "ceda":
    from ceda import ConsumerSearchClient
else:
    from globus import ConsumerSearchClient


logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO
)


if __name__ == "__main__":
    message_processor = ConsumerSearchClient()

    consumer_service = KafkaConsumerService(
        event_stream.get("config"),
        event_stream.get("topics"),
        message_processor,
    )
    consumer_service.start()
