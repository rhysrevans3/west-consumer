from pydantic_settings import BaseSettings


class GlobusClientSettings(BaseSettings):
    """
    Globus settings
    """

    node: str

    client_id: str
    client_secret: str
    search_index: str
    stac_server: str
