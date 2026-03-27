from pydantic_settings import BaseSettings


class CEDAClientSettings(BaseSettings):
    """
    CEDA settings
    """

    client_id: str
    client_secret: str
    stac_server: str
    token_url: str
