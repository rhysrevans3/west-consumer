from typing import Any, Literal

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from settings.ceda import CEDAClientSettings
from settings.globus import GlobusClientSettings


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
    client: CEDAClientSettings | GlobusClientSettings = Field(
        discriminator="client_type"
    )

    debug: bool = False

    @model_validator(mode="before")
    @classmethod
    def set_client_type(cls, data: Any) -> Any:
        """set authorizer as client type.

        Args:
            data (Any): model data

        Returns:
            Any: data with updated client type
        """
        data["client"]["client_type"] = data["node"]
        return data


settings = Settings()
