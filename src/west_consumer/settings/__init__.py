import os
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

if os.environ.get("CONSUMER_NODE") == "ceda":
    from west_consumer.settings.ceda import CEDAClientSettings as ClientSettings
else:
    from west_consumer.settings.globus import GlobusClientSettings as ClientSettings


class Settings(BaseSettings):
    """
    Event Stream Settings
    """

    model_config = SettingsConfigDict(
        env_prefix="CONSUMER_",
        env_nested_delimiter="__",
        env_file=".env",
    )

    node: Literal["ceda", "globus"]
    client: ClientSettings = Field(discriminator="client_type")

    debug: bool = False


settings = Settings()
