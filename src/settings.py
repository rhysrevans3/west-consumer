import os

from pydantic_settings import BaseSettings, SettingsConfigDict

from utils import get_secret

# ESGF2 Globus Project
project_id = "cae45630-2a4b-47b9-b704-d870e341da67"

# ESGF2 STAC Transaction API client
publisher = {
    "client_id": "ec5f07c0-7ed8-4f2b-94f2-ddb6f8fc91a3",
    "redirect_uri": "https://auth.globus.org/v2/web/auth-code",
}

consumer = "globus"

# ESGF2 STAC Transaction API service
stac_api = {
    "client_id": "6fa3b827-5484-42b9-84db-f00c7a183a6a",
    "client_secret": os.environ.get("CLIENT_SECRET"),
    "issuer": "https://auth.globus.org",
    "access_control_policy": "https://esgf2.s3.amazonaws.com/access_control_policy.json",
    "admins": "https://esgf2.s3.amazonaws.com/admins.json",
    "scope_id": "ca49f459-a4f8-420c-b55f-194df11abc0f",
    "scope_string": "https://auth.globus.org/scopes/6fa3b827-5484-42b9-84db-f00c7a183a6a/ingest",
    "url": "https://n08bs7a0hc.execute-api.us-east-1.amazonaws.com/dev",
}

# ESGF2 Event Stream Service Producer/Consumer
kafka_secretsmanager = {
    "region_name": "us-east-1",
    "secret_name": os.environ.get("SECRET_NAME"),
}
sasl_secret = get_secret(kafka_secretsmanager)

event_stream = {
    "config": {
        "bootstrap.servers": "b-1.esgf2a.3wk15r.c9.kafka.us-east-1.amazonaws.com:9096,"
        "b-2.esgf2a.3wk15r.c9.kafka.us-east-1.amazonaws.com:9096,"
        "b-3.esgf2a.3wk15r.c9.kafka.us-east-1.amazonaws.com:9096",
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": sasl_secret.get("username"),
        "sasl.password": sasl_secret.get("password"),
        "group.id": "westconsumer",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    },
    "topics": ["esgf2"],
}

if os.environ.get("KAFKA_CLIENT_DEBUG"):
    event_stream["config"]["debug"] = "all"
    event_stream["config"]["log_level"] = 7

# ESGF2 West Consumer
globus_secretsservice = {
    "region_name": "us-east-1",
    "secret_name": os.environ.get("GLOBUS_SECRET_NAME"),
}
globus_search_client_credentials = get_secret(globus_secretsservice)

# ESGF2 Globus Search
globus_search = {
    # "index": "d7814ff7-51a9-4155-8b84-97e84600acd7",
    "index": "f037bb33-3413-448b-8486-8400bee5181a",
}


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
