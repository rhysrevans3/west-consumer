import logging
from typing import Any
from urllib.parse import urljoin

import httpx
from esgf_playground_utils.models.item import CMIP6Item
from esgf_playground_utils.models.kafka import CreatePayload, KafkaEvent, PatchPayload, RevokePayload, UpdatePayload
from httpx_auth import OAuth2ClientCredentials
from stac_fastapi.extensions.core.transaction.request import PartialItem, PatchOperation

from settings import CEDAClientSettings


class ConsumerSearchClient:
    """
    CEDA Kafka Comsumer Client
    """

    def __init__(self, error_producer):
        self.settings = CEDAClientSettings()
        self.auth = OAuth2ClientCredentials(
            self.settings.token_url,
            self.settings.client_id,
            self.settings.client_secret,
        )
        self.client = httpx.Client(timeout=5.0, verify=False)
        self.error_producer = error_producer

    def create_item(
        self,
        collection_id: str,
        item: CMIP6Item,
    ) -> None:
        """Create item

        Args:
            collection_id (str): item's collection ID
            item (CMIP6Item): item to be generated
        """

        url = urljoin(
            self.settings.stac_server,
            f"collections/{collection_id}/items",
        )

        logging.info("Posting %s to %s", item.id, url)
        response = self.client.post(
            url,
            data=item.model_dump_json(),
            auth=self.auth,
        )

        if response.is_success:
            logging.info("Item %s succesfully posted", item.id)

        else:
            logging.info("Item %s failed to post: %s", item.id, response.content)
            self.error_producer.produce(
                topic="esgf-local.errors",
                key=item.id,
                value=response.content,
            )

    def patch_item(self, collection_id: str, item_id: str, patch: PartialItem | list[PatchOperation]):
        """Patch Item

        Args:
            collection_id (str): item's collection ID
            item_id (str): item's ID
            patch (PartialItem | list[PatchOperation]): partial item or list of patch operations
        """
        url = urljoin(
            self.settings.stac_server,
            f"collections/{collection_id}/items/{item_id}",
        )

        logging.info("Patching %s to %s", item_id, url)
        response = self.client.put(
            url,
            json=[op.model_dump() for op in patch] if isinstance(patch, list) else patch.model_dump(),
            auth=self.auth,
        )

        if response.is_success:
            logging.info("Item %s succesfully update", item_id)

        else:
            logging.info("Item %s failed to update: %s", item_id, response.content)
            self.error_producer.produce(
                topic="esgf-local.errors",
                key=item_id,
                value=response.content,
            )

    def update_item(
        self,
        collection_id: str,
        item_id: str,
        item: CMIP6Item,
    ) -> None:
        """Update item

        Args:
            collection_id (str): item's collection ID
            item_id (str): item's ID
            item (CMIP6Item): item to be updated
        """

        url = urljoin(
            self.settings.stac_server,
            f"collections/{collection_id}/items/{item_id}",
        )

        logging.info("Updating %s to %s", item_id, url)
        response = self.client.put(
            url,
            json=item.model_dump(),
            auth=self.auth,
        )

        if response.is_success:
            logging.info("Item %s succesfully update", item_id)

        else:
            logging.info("Item %s failed to update: %s", item_id, response.content)
            self.error_producer.produce(
                topic="esgf-local.errors",
                key=item_id,
                value=response.content,
            )

    def delete_item(
        self,
        collection_id: str,
        item_id: str,
    ) -> None:
        """Delete item

        Args:
            collection_id (str): item's collection ID
            item_id (str): item's ID
        """

        url = urljoin(
            self.settings.stac_server,
            f"collections/{collection_id}/items/{item_id}",
        )

        logging.info("Deleting %s at %s", item_id, url)
        response = self.client.delete(url, auth=self.auth)

        if response.is_success:
            logging.info("Item %s succesfully deleted", item_id)

        else:
            logging.info("Item %s failed to delete: %s", item_id, response.content)
            self.error_producer.produce(
                topic="esgf-local.errors",
                key=item_id,
                value=response.content,
            )

    def ingest(self, events: list[dict[str, Any]]) -> bool:
        """Ingest Kafka events

        Args:
            events (list[dict[str, Any]]): Events to be ingested

        Returns:
            bool: true if ingestion successful
        """

        for data in events:
            event = KafkaEvent.model_validate(data)

            match event.data.payload:

                case CreatePayload():
                    self.create_item(
                        collection_id=event.data.payload.collection_id,
                        item=event.data.payload.item,
                    )
                    logging.info("Item %s created.", event.data.payload.item.id)

                case UpdatePayload():
                    self.update_item(
                        collection_id=event.data.payload.collection_id,
                        item_id=event.data.payload.item_id,
                        item=event.data.payload.item,
                    )
                    logging.info("Item %s updated.", event.data.payload.item.id)

                case PatchPayload():
                    self.patch_item(
                        collection_id=event.data.payload.collection_id,
                        item_id=event.data.payload.item_id,
                        patch=event.data.payload.patch,
                    )
                    logging.info("Item %s patched.", event.data.payload.item_id)

                case RevokePayload(method="DELETE"):
                    self.delete_item(
                        collection_id=event.data.payload.collection_id,
                        item_id=event.data.payload.item_id,
                    )
                    logging.info("Item %s deleted.", event.data.payload.item_id)

        return True
