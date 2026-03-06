from pydantic_settings import BaseSettings, SettingsConfigDict

from settings.kafka import KafkaConsumerConfig


class ProducerSettings(BaseSettings):
    """
    Event Stream Settings
    """

    model_config = SettingsConfigDict(
        validate_by_name=True,
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="PRODUCER_",
    )

    node: str = "ceda"
    config: KafkaConsumerConfig
    error_topic: str
    success_topic: str


producer_settings = ProducerSettings()
