from pydantic_settings import BaseSettings, SettingsConfigDict


class CEDAClientSettings(BaseSettings):
    """
    CEDA settings
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
    )

    token_url: str
    client_id: str
    client_secret: str
    stac_server: str


ceda_client_settings = CEDAClientSettings()
