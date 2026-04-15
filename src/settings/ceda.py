from typing import Literal

from pydantic import BaseModel


class CEDAClientSettings(BaseModel):
    """
    CEDA settings
    """

    client_type: Literal["ceda"]
    client_id: str
    client_secret: str
    token_url: str

    stac_server: str

    slack_hook: str | None = None
