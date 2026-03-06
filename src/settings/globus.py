from pydantic_settings import BaseSettings


class GlobusClientSettings(BaseSettings):
    """
    CEDA settings
    """

    client_id: str
    client_secret: str
    search_index: str
    stac_server: str


globus_client_settings = GlobusClientSettings()
