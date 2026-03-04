import os
import socket

from dotenv import load_dotenv

# Load the .env file
load_dotenv()

run_environment = os.environ.get("RUN_ENVIRONMENT", "local")

# ESGF2 Event Stream Service Consumer
if run_environment == "local":
    event_stream = {
        "config": {
            "auto.offset.reset": "earliest",
            "bootstrap.servers": "host.docker.internal:9092",
            "client.id": socket.gethostname(),
            "enable.auto.commit": False,
            "group.id": "westconsumer",
        },
        "topics": ["esgf-local.transactions"],
    }
else:
    event_stream = {
        "config": {
            "auto.offset.reset": "earliest",
            "bootstrap.servers": os.environ.get("BOOTSTRAP_SERVERS"),
            "enable.auto.commit": False,
            "group.id": "westconsumer",
            "sasl.mechanism": "PLAIN",
            "sasl.username": os.environ.get("CONFLUENT_CLOUD_USERNAME"),
            "sasl.password": os.environ.get("CONFLUENT_CLOUD_PASSWORD"),
            "security.protocol": "SASL_SSL",
        },
        "topics": [os.environ.get("TOPICS")],
    }

if os.environ.get("KAFKA_CLIENT_DEBUG", False):
    event_stream["config"]["debug"] = "all"
    event_stream["config"]["log_level"] = 7

globus_search_client_credentials = {
    "client_id": os.environ.get("CLIENT_ID"),
    "client_secret": os.environ.get("CLIENT_SECRET"),
}

# ESGF2 Globus Search
globus_search = {"index": os.environ.get("GLOBUS_SEARCH_INDEX")}
