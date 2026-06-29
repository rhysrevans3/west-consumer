import json
import logging
import traceback
import uuid
from datetime import datetime, timezone
from importlib.metadata import version
from urllib.parse import urljoin

import httpx
from confluent_kafka import KafkaException
from confluent_kafka import Message as KafkaMessage
from esgf_core_utils.models.kafka.events import (
    CreatePayload,
    KafkaErrorEvent,
    KafkaEvent,
    KafkaSuccessEvent,
    PatchPayload,
    UpdatePayload,
)
from esgf_core_utils.models.kafka.message_processor import MessageProcessor
from esgf_core_utils.models.kafka.producer import KafkaProducer
from httpx_auth import OAuth2ClientCredentials
from pydantic_core import ValidationError
from tenacity import (
    before_sleep_log,
    retry,
    stop_after_attempt,
    wait_exponential_jitter,
)

from west_consumer.settings import settings


class CEDAMessageProcessor(MessageProcessor):
    """
    CEDA Message Processor
    """

    def __init__(self):
        self.auth = OAuth2ClientCredentials(
            settings.client.token_url,
            settings.client.client_id,
            settings.client.client_secret,
        )
        self.client = httpx.Client(timeout=5.0, verify=False)
        self.producer = KafkaProducer()

    @retry(
        wait=wait_exponential_jitter(max=settings.client.max_retry_time),
        stop=stop_after_attempt(settings.client.max_retries),
        before_sleep=before_sleep_log(logging, logging.WARNING),
        reraise=True,
    )
    def create_item(
        self,
        event: KafkaEvent,
        result_event: KafkaSuccessEvent,
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
                headers={"Content-Type": "application/json"},
            )

            response.raise_for_status()

            logging.info("SUCCESS: CREATE Item %s", item.id)

            self.producer.success(
                key=item.id,
                value=result_event.model_dump_json().encode("utf8"),
            )

        except httpx.HTTPStatusError as exc:
            logging.error("FAIL: CREATE Item %s: %s", item.id, response.content)
            if (
                "code" in response.json()
                and response.json()["code"] == "ItemAlreadyExistsError"
            ):
                error_event = KafkaErrorEvent(
                    error={
                        "detail": exc.response.content,
                        "instance": event.metadata.request_id,
                        "status": exc.response.status_code,
                        "title": f"{item.id} already exists",
                        "type": "ItemAlreadyExists",
                    },
                    **result_event.model_dump(),
                )

                self.producer.error(
                    key=item.id,
                    value=error_event.model_dump_json().encode("utf-8"),
                )
            else:
                raise

    @retry(
        wait=wait_exponential_jitter(max=settings.client.max_retry_time),
        stop=stop_after_attempt(settings.client.max_retries),
        before_sleep=before_sleep_log(logging, logging.WARNING),
        reraise=True,
    )
    def patch_item(
        self,
        event: KafkaEvent,
        result_event: KafkaSuccessEvent,
    ) -> None:
        """Patch Item

        Args:
            event (KafkaEvent): event to be processed
        """

        try:
            collection_id = event.data.payload.collection_id
            item_id = event.data.payload.item_id
            patch = event.data.payload.patch

            url = urljoin(
                settings.client.stac_server,
                f"collections/{collection_id}/items/{item_id}",
            )

            logging.info("Patch %s", patch)
            content_type = (
                "application/json-patch+json"
                if isinstance(patch, list)
                else "application/merge-patch+json"
            )

            data = (
                [op.model_dump() for op in patch]
                if isinstance(patch, list)
                else patch.model_dump(exclude_unset=True, exclude_defaults=True)
            )

            response = self.client.patch(
                url,
                json=data,
                auth=self.auth,
                headers={"Content-Type": content_type},
            )

            response.raise_for_status()

            logging.info("SUCCESS: PATCH Item %s", item_id)
            self.producer.success(
                key=item_id,
                value=result_event.model_dump_json().encode("utf8"),
            )

        except httpx.HTTPStatusError as exc:
            if response.json()["code"] == "NotFoundError":
                logging.error("FAIL: PATCH Item %s: %s", item_id, response.content)

                error_event = KafkaErrorEvent(
                    error={
                        "detail": exc.response.content,
                        "instance": event.metadata.request_id,
                        "status": exc.response.status_code,
                        "title": f"{item_id} already exists",
                        "type": "ItemAlreadyExists",
                    },
                    **result_event.model_dump(),
                )
                self.producer.error(
                    key=item_id,
                    value=error_event.model_dump_json().encode("utf-8"),
                )

            else:
                raise

    @retry(
        wait=wait_exponential_jitter(max=settings.client.max_retry_time),
        stop=stop_after_attempt(settings.client.max_retries),
        before_sleep=before_sleep_log(logging, logging.WARNING),
        reraise=True,
    )
    def update_item(
        self,
        event: KafkaEvent,
        result_event: KafkaSuccessEvent,
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
            self.producer.success(
                key=item_id,
                value=result_event.model_dump_json().encode("utf8"),
            )

        except httpx.HTTPStatusError as exc:
            if response.json()["code"] == "NotFoundError":
                logging.error("FAIL: UPDATE Item %s: %s", item_id, exc)

                error_event = KafkaErrorEvent(
                    error={
                        "detail": exc.response.content,
                        "instance": event.metadata.request_id,
                        "status": exc.response.status_code,
                        "title": f"{item_id} already exists",
                        "type": "ItemAlreadyExists",
                    },
                    **result_event.model_dump(),
                )
                self.producer.error(
                    key=item_id,
                    value=error_event.model_dump_json().encode("utf-8"),
                )

            else:
                raise

    @retry(
        wait=wait_exponential_jitter(max=settings.client.max_retry_time),
        stop=stop_after_attempt(settings.client.max_retries),
        before_sleep=before_sleep_log(logging, logging.WARNING),
        reraise=True,
    )
    def delete_item(
        self,
        event: KafkaEvent,
        result_event: KafkaSuccessEvent,
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
            self.producer.success(
                key=item_id,
                value=result_event.model_dump_json().encode("utf8"),
            )

        except httpx.HTTPStatusError as exc:
            if response.json()["code"] == "NotFoundError":
                logging.error("FAILED: DELETE Item %s: %s", item_id, response.content)

                error_event = KafkaErrorEvent(
                    error={
                        "detail": exc.response.content,
                        "instance": event.metadata.request_id,
                        "status": exc.response.status_code,
                        "title": f"{item_id} already exists",
                        "type": "ItemAlreadyExists",
                    },
                    **result_event.model_dump(),
                )
                self.producer.error(
                    key=item_id,
                    value=error_event.model_dump_json().encode("utf-8"),
                )

            else:
                raise

    def load_event(self, message: KafkaMessage) -> tuple[KafkaEvent, KafkaSuccessEvent]:
        """Load event from message

        Args:
            message (KafkaMessage): message from kafka stream

        Returns:
            KafkaEvent: STAC event
            KafkaSuccessEvent:
        """
        try:
            data = json.loads(message.value().decode("utf8"))
            event = KafkaEvent.model_validate(data)

            result_event = KafkaSuccessEvent(
                data={
                    "type": event.data.type,
                    "payload": {
                        "collection_id": event.data.payload.collection_id,
                        "method": event.data.payload.method,
                        "item_id": (
                            event.data.payload.item.id
                            if isinstance(event.data.payload, CreatePayload)
                            else event.data.payload.item_id
                        ),
                    },
                },
                metadata={
                    "event_id": uuid.uuid4().hex,
                    "request_id": event.metadata.request_id,
                    "auth": event.metadata.auth,
                    "publisher": {
                        "package": "west-consumer",
                        "version": version("west-consumer"),
                    },
                    "time": datetime.now().isoformat(),
                    "schema_version": event.metadata.schema_version,
                },
                original_event={
                    "event_id": event.metadata.event_id,
                    "offset": message.offset(),
                    "partition": message.partition(),
                },
            )

            return event, result_event

        except ValidationError as e:
            logging.error(
                "Validation error at offset %s | partition %s : %s.",
                message.offset(),
                message.partition(),
                e,
            )
            raise

    def handle_event(self, event: KafkaEvent, result_event: KafkaSuccessEvent) -> None:
        """Handle STAC event

        Args:
            event (KafkaEvent): STAC event

        Raises:
            Exception: Failed to match case
        """
        match event.data.payload:

            case CreatePayload():
                logging.info("ATTEMPT CREATE Item: %s", event.data.payload.item.id)
                self.create_item(event=event, result_event=result_event)

            case UpdatePayload():
                logging.info("ATTEMPT UPDATE Item: %s", event.data.payload.item_id)
                self.update_item(event=event, result_event=result_event)

            case PatchPayload():
                logging.info("ATTEMPT PATCH Item: %s", event.data.payload.item_id)
                self.patch_item(event=event, result_event=result_event)

            case _:
                logging.error(
                    "FAILED: No Payload match found for : %s",
                    event.data.payload.item_id,
                )
                raise ValueError(f"No Payload match found for : {event}")

    def post_to_slack(self, message: KafkaMessage, error: Exception) -> None:
        """Post kafka message and error to Slack

        Args:
            message (KafkaMessage): kafka message that failed
            error (Exception): error message to post
        """
        try:
            if settings.client.slack_hook:
                payload = {
                    "text": (
                        f"*Message Offset:* {message.offset()}\n"
                        f"*Message Partition:* {message.partition()}\n"
                        f"*Type:* `{type(error).__name__}`\n"
                        f"*Error:* `{str(error)}`\n"
                    ),
                    "blocks": [
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": (
                                    f"*MESSAGE*"
                                    f"```json\n{json.dumps(message.value().decode('utf8'))}\n```"
                                ),
                            },
                        }
                    ],
                }

                httpx.post(
                    settings.client.slack_hook,
                    headers={"Content-Type": "application/json"},
                    data=json.dumps(payload),
                )

        except httpx.HTTPError as exc:
            logging.error("Failed posting to Slack: %s", exc)

    def ingest(self, message: KafkaMessage) -> None:
        """Ingest Kafka events

        Args:
            events (list[dict[str, Any]]): Events to be ingested
        """

        try:
            if message.error():
                logging.error(
                    "Message error at offset %s | partition %s : %s.",
                    message.offset(),
                    message.partition(),
                    message.error(),
                )
                logging.error(
                    "Message data %s.",
                    message.value(),
                )
                raise KafkaException(message.error())

            event, result_event = self.load_event(message=message)

            self.handle_event(event=event, result_event=result_event)

        except Exception as exc:
            logging.error("Failed to process event: %s", message.value())
            logging.error(
                "Failed to process event: %s",
                {
                    "type": type(exc).__name__,
                    "error": str(exc),
                    "traceback": traceback.format_exc(),
                },
            )

            self.post_to_slack(message=message, error=exc)
            raise exc
