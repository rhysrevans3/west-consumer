from pydantic import BaseModel


class CEDAClientSettings(BaseModel):
    """
    CEDA settings
    """

    client_id: str
    client_secret: str
    token_url: str

    stac_server: str

    max_retries: int = 3
    max_retry_time: int = 60

    slack_hook: str | None = None
