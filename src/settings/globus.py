from typing import Literal

from pydantic import BaseModel


class GlobusClientSettings(BaseModel):
    """
    Globus settings
    """

    client_id: str
    client_secret: str
    search_index: str

    stac_server: str
