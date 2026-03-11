from pydantic_settings import BaseSettings

from settings.kafka import KafkaConsumerConfig


class ProducerSettings(BaseSettings):
    """
    Event Stream Settings
    """

    class Config:
        validate_by_name = True
        env_prefix = "PRODUCER_"

    node: str = "ceda"
    kafka_config: KafkaConsumerConfig
    error_topic: str
    success_topic: str


producer_settings = ProducerSettings()
