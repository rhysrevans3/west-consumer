from pydantic_settings import BaseSettings

from src.settings.ceda import CEDAClientSettings
from src.settings.globus import GlobusClientSettings


class Settings(BaseSettings):
    """
    Settings
    """

    node: str

    slack_hook: str | None

    client: CEDAClientSettings | GlobusClientSettings


settings = Settings()
