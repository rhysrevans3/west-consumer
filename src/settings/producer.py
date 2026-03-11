from pydantic_settings import BaseSettings

from settings.kafka import KafkaConfig


class ProducerSettings(BaseSettings):
    """
    Event Stream Settings
    """

    class Config:
        validate_by_name = True
        env_prefix = "PRODUCER_"
        extra = "ignore"

    node: str = "ceda"
    kafka_config: KafkaConfig
    error_topic: str
    success_topic: str


producer_settings = ProducerSettings()
