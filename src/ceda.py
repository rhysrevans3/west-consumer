import json
import logging
from datetime import datetime, timezone
from urllib.parse import urljoin

import httpx
from confluent_kafka import Message as KafkaMessage
from esgf_core_utils.models.kafka.events import (
    CreatePayload,
    KafkaErrorEvent,
    KafkaEvent,
    PatchPayload,
    UpdatePayload,
)
from esgf_core_utils.models.kafka.producer import KafkaProducer
from httpx_auth import OAuth2ClientCredentials
from pydantic_core import ValidationError

from settings import settings


class ConsumerSearchClient:
    """
    CEDA Kafka Comsumer Client
    """

    def __init__(self):
        self.auth = OAuth2ClientCredentials(
            settings.client.token_url,
            settings.client.client_id,
            settings.client.client_secret,
        )
        self.client = httpx.Client(timeout=5.0, verify=False)
        self.producer = KafkaProducer()

    def create_item(
        self,
        event: KafkaEvent,
    ) -> None:
        """Create item

        Args:
            event (KafkaEvent): event to be processed
        """
        try:
            collection_id = event.data.payload.collection_id
            item = event.data.payload.item

            url = urljoin(
                settings.client.stac_server,
                f"collections/{collection_id}/items",
            )

            now = datetime.now(timezone.utc)
            setattr(item.properties, "created", now)
            setattr(item.properties, "updated", now)

            response = self.client.post(
                url,
                data=item.model_dump_json(exclude_unset=True, exclude_defaults=True),
                auth=self.auth,
            )

            response.raise_for_status()

            logging.info("SUCCESS: CREATE Item %s", item.id)
            self.producer.produce(
                topic=settings.success_topic,
                key=item.id,
                value=event.model_dump_json().encode("utf8"),
            )

        except httpx.HTTPError as exc:
            logging.error("FAIL: CREATE Item %s: %s", item.id, response.content)
            if response.json()["code"] == "ItemAlreadyExistsError":
                error_event = KafkaErrorEvent(Error={"traceback": exc}, event=event)

                self.producer.produce(
                    topic=settings.error_topic,
                    key=item.id,
                    value=error_event,
                )

            raise

    def patch_item(
        self,
        event: KafkaEvent,
    ) -> None:
        """Patch Item

        Args:
            event (KafkaEvent): event to be processed
        """
        try:
            collection_id = (event.data.payload.collection_id,)
            item_id = event.data.payload.item_id
            patch = event.data.payload.patch

            url = urljoin(
                settings.client.stac_server,
                f"collections/{collection_id}/items/{item_id}",
            )

            logging.info("Patch %s", patch)
            response = self.client.patch(
                url,
                json=patch,
                # data=[op.model_dump_json() for op in patch] if isinstance(patch, list) else patch.model_dump_json(exclude_unset=True),
                auth=self.auth,
                headers={"Content-Type": "application/json-patch+json"},
            )

            response.raise_for_status()

            logging.info("SUCCESS: PATCH Item %s", item_id)
            self.producer.produce(
                topic=settings.success_topic,
                key=item_id,
                value=event.model_dump_json().encode("utf8"),
            )

        except httpx.HTTPError as exc:
            if response.json()["code"] == "NotFoundError":
                logging.error("FAIL: PATCH Item %s: %s", item_id, response.content)

                error_event = KafkaErrorEvent(Error={"traceback": exc}, event=event)
                self.producer.produce(
                    topic=settings.error_topic,
                    key=item_id,
                    value=error_event,
                )

            else:
                raise

    def update_item(
        self,
        event: KafkaEvent,
    ) -> None:
        """Update item

        Args:
            event (KafkaEvent): event to be processed
        """
        try:
            collection_id = event.data.payload.collection_id
            item_id = event.data.payload.item_id
            item = event.data.payload.item

            url = urljoin(
                settings.client.stac_server,
                f"collections/{collection_id}/items/{item_id}",
            )

            response = self.client.put(
                url,
                data=item.model_dump_json(exclude_unset=True, exclude_defaults=True),
                auth=self.auth,
            )

            response.raise_for_status()

            logging.info("SUCCESS: UPDATE Item %s", item_id)
            self.producer.produce(
                topic=settings.success_topic,
                key=item_id,
                value=event.model_dump_json().encode("utf8"),
            )

        except httpx.HTTPError as exc:
            if response.json()["code"] == "NotFoundError":
                logging.error("FAIL: UPDATE Item %s: %s", item_id, exc)

                error_event = KafkaErrorEvent(Error={"traceback": exc}, event=event)
                self.producer.produce(
                    topic=settings.error_topic,
                    key=item_id,
                    value=error_event,
                )

            else:
                raise

    def delete_item(
        self,
        event: KafkaEvent,
    ) -> None:
        """Delete item

        Args:
            collection_id (str): item's collection ID
            item_id (str): item's ID
        """
        try:
            collection_id = event.data.payload.collection_id
            item_id = event.data.payload.item_id
            url = urljoin(
                settings.client.stac_server,
                f"collections/{collection_id}/items/{item_id}",
            )

            response = self.client.delete(url, auth=self.auth)

            response.raise_for_status()

            logging.info("SUCCESS: DELETE Item %s", item_id)
            self.producer.produce(
                topic=settings.success_topic,
                key=item_id,
                value=event.model_dump_json().encode("utf8"),
            )

        except httpx.HTTPError as exc:
            if response.json()["code"] == "NotFoundError":
                logging.error("FAILED: DELETE Item %s: %s", item_id, response.content)

                error_event = KafkaErrorEvent(Error={"traceback": exc}, event=event)
                self.producer.produce(
                    topic=settings.error_topic,
                    key=item_id,
                    value=error_event,
                )

            else:
                raise

    def ingest(self, message: KafkaMessage) -> None:
        """Ingest Kafka events

        Args:
            events (list[dict[str, Any]]): Events to be ingested
        """
        try:
            try:
                data = json.loads(message.value().decode("utf8"))
                event = KafkaEvent.model_validate(data)
                setattr(event.metadata, "node", settings.client.node)

            except ValidationError as e:
                logging.error(
                    "Validation error at offset %s: %s.",
                    message.offset(),
                    e,
                )
                raise

            match event.data.payload:

                case CreatePayload():
                    logging.info("ATTEMPT CREATE Item: %s", event.data.payload.item.id)
                    self.create_item(event=event)

                case UpdatePayload():
                    logging.info("ATTEMPT UPDATE Item: %s", event.data.payload.item_id)
                    self.update_item(event=event)

                case PatchPayload():
                    logging.info("ATTEMPT PATCH Item: %s", event.data.payload.item_id)
                    self.patch_item(event=event)

                case _:
                    logging.error(
                        "FAILED: No Payload match found for : %s",
                        event.data.payload.item_id,
                    )
                    raise Exception(f"No Payload match found for : {event}")

        except Exception as exc:
            try:
                logging.error("Failed to process event: %s", event)

                if settings.slack_hook:
                    payload = {
                        "kafka_message": message,
                        "error": exc,
                    }

                    httpx.post(
                        settings.slack_hook,
                        headers={"Content-Type": "application/json"},
                        json={"text": json.dumps(payload)},
                    )

            except Exception as e:
                logging.error("Failed posting to Slack: %s", e)
