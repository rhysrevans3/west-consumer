from typing import Self

from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from settings.kafka import KafkaConsumerConfig


class ConsumerSettings(BaseSettings):
    """
    Event Stream Settings
    """

    model_config = SettingsConfigDict(
        validate_by_name=True,
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="CONSUMER_",
    )

    node: str = "ceda"
    config: KafkaConsumerConfig
    topics: list[str]
    timeout: float = 5.0
    slack_hook: str | None = None

    debug: bool = False

    @model_validator(mode="after")
    def check_debug(self) -> Self:
        """
        Check if debug is set if so update kafka config.
        """
        if self.debug:
            if self.config.debug is None:
                setattr(self.config, "debug", "all")

            if self.config.level is None:
                setattr(self.config, "level", 7)

        return self

    @field_validator("topics", mode="before")
    @classmethod
    def split_topics(cls, v):
        """
        Accept comma-separated string or list.
        """
        if isinstance(v, str):
            return [t.strip() for t in v.split(",") if t.strip()]

        if isinstance(v, (list, tuple)):
            return list(v)

        raise TypeError("topics must be a string or list")


consumer_settings = ConsumerSettings()
