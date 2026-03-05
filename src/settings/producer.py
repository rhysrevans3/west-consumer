import logging
import os
import socket

from dotenv import load_dotenv

# Load the .env file
load_dotenv()

# Suppress some kafka message streams
logger = logging.getLogger("kafka")
logger.setLevel(logging.WARN)

run_environment = os.environ.get("RUN_ENVIRONMENT", None)


# Kafka connection details
if run_environment == "local":
    error_event_stream = {
        "config": {
            "bootstrap.servers": "host.docker.internal:9092",
            "client.id": socket.gethostname(),
        },
        "topic": "esgf-local.errors",
    }
else:
    error_event_stream = {
        "config": {
            "bootstrap.servers": os.environ.get("BOOTSTRAP_SERVERS"),
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": os.environ.get("CONFLUENT_CLOUD_USERNAME"),
            "sasl.password": os.environ.get("CONFLUENT_CLOUD_PASSWORD"),
        },
        "topic": os.environ.get("TOPIC", "esgf2.data-challenges.01.errors"),
    }
