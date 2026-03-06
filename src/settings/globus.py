from pydantic_settings import BaseSettings, SettingsConfigDict


class GlobusClientSettings(BaseSettings):
    """
    CEDA settings
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
    )

    client_id: str
    client_secret: str
    search_index: str
    stac_server: str


globus_client_settings = GlobusClientSettings()
