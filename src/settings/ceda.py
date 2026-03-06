from pydantic_settings import BaseSettings


class CEDAClientSettings(BaseSettings):
    """
    CEDA settings
    """

    token_url: str
    client_id: str
    client_secret: str
    stac_server: str


ceda_client_settings = CEDAClientSettings()
