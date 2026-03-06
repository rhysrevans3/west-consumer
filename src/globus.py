import logging
import time

import jsonpatch
from globus_sdk import (
    ClientCredentialsAuthorizer,
    ConfidentialAppAuthClient,
    SearchClient,
)
from globus_sdk.scopes import SearchScopes
from globus_sdk.services.search.errors import SearchAPIError

from producer import KafkaProducer
from settings.globus import globus_client_settings


class ConsumerSearchClient:
    def __init__(self):
        confidential_client = ConfidentialAppAuthClient(
            client_id=globus_client_settings.client_id,
            client_secret=globus_client_settings.client_secret,
        )
        authorizer = ClientCredentialsAuthorizer(
            confidential_client,
            scopes=SearchScopes.all,
        )
        self.search_client = SearchClient(authorizer=authorizer)
        self.esgf_index = globus_client_settings.search_index
        self.error_producer = KafkaProducer()

    def normalize_assets(self, assets):
        normalized_assets = []
        for key, value in assets.items():
            normalized_assets.append({"name": key} | value)
        for asset in normalized_assets:
            if "alternate" in asset and asset.get("alternate"):
                asset["alternate"] = self.normalize_assets(asset["alternate"])
        return normalized_assets

    def denormalize_assets(self, assets):
        denormalized_assets = {}
        for asset in assets:
            name = asset.pop("name")
            if "alternate" in asset:
                asset["alternate"] = self.denormalize_assets(asset["alternate"])
            else:
                asset["alternate"] = {}
            denormalized_assets[name] = asset
        return denormalized_assets

    def gmetaentry(self, item):
        return {
            "subject": item.get("id"),
            "visible_to": [
                "public",
            ],
            "content": item,
        }

    def get_index(self):
        return self.search_client.get_index(self.esgf_index)

    def search(self, query):
        return self.search_client.search(self.esgf_index, query)

    def ingest(self, messages_data):
        gmeta = []
        for data in messages_data:
            item = data.get("data").get("payload").get("item")
            item["assets"] = item.get("assets")
            gmeta.append(self.gmetaentry(item))

        gmetalist = {"ingest_type": "GMetaList", "ingest_data": {"gmeta": gmeta}}

        r = self.search_client.ingest(self.esgf_index, gmetalist)
        task_id = r.get("task_id")

        while True:
            r = self.search_client.get_task(task_id)
            state = r.get("state")
            if state == "SUCCESS":
                return True
            if state == "FAILED":
                logging.error(f"Ingestion task {task_id} failed")
                logging.error(r.text)
                return False
            time.sleep(1)
        return True

    def post(self, message_data):
        item = message_data.get("data").get("payload").get("item")
        try:
            globus_response = self.search_client.get_subject(
                self.esgf_index, item.get("id")
            )
        except SearchAPIError as e:
            if e.http_status == 404:
                item["assets"] = self.normalize_assets(item.get("assets"))
                gmeta_entry = self.gmetaentry(item)
                return gmeta_entry

        if globus_response.data:
            logging.info(f"Item with ID {item.get('id')} already exists in the index.")
            self.error_producer.produce(
                topic="esgf-local.errors",
                key=item.get("id"),
                value=f"Item with ID {item.get('id')} already exists in the index.",
            )
            print("Item already exists, returning None")
            return None
        return None

    def json_patch(self, message_data):
        payload = message_data.get("data").get("payload")
        item_id = payload.get("item_id")
        globus_response = self.search_client.get_subject(self.esgf_index, item_id)
        if not globus_response.data:
            logging.info(f"Item with ID {item_id} does not exist in the index.")
            self.error_producer.produce(
                topic="esgf-local.errors",
                key=item_id,
                value=f"Item with ID {item_id} does not exist in the index.",
            )
            return None
        item = globus_response.data.get("entries")[0].get("content")
        item["assets"] = self.denormalize_assets(item.get("assets"))
        patched_item = jsonpatch.apply_patch(item, payload.get("patch"))
        patched_item["assets"] = self.normalize_assets(patched_item.get("assets"))
        gmeta_entry = self.gmetaentry(patched_item)
        return gmeta_entry

    def delete(self, subject):
        globus_response = self.search_client.get_subject(self.esgf_index, subject)
        if globus_response.data:
            self.search_client.delete_subject(self.esgf_index, subject)
            return True
        logging.info(f"Item with ID {subject} does not exist in the index.")
        self.error_producer.produce(
            topic="esgf-local.errors",
            key=subject,
            value=f"Item with ID {subject} does not exist in the index.",
        )
        return None

    def process_message(self, message_data):
        try:
            payload = message_data.get("data").get("payload")
            method = payload.get("method")
            print(f"Processing message with method: {method}")
            if method == "POST":
                return self.post(message_data)
            if method == "PUT":
                return self.put(message_data)
            if method == "JSON_PATCH" or method == "PATCH":
                return self.json_patch(message_data)
            return None
        except Exception as e:
            logging.error(f"Error processing message data: {e}")
            self.error_producer.produce(
                topic="esgf-local.errors",
                key=message_data.get("data").get("payload").get("item").get("id"),
                value=str(e),
            )
            return None

    def process_messages(self, messages_data):
        gmeta = []
        for message_data in messages_data:
            entry = self.process_message(message_data)
            if entry:
                gmeta.append(entry)
        if not gmeta:
            return True

        gmetalist = {"ingest_type": "GMetaList", "ingest_data": {"gmeta": gmeta}}

        r = self.search_client.ingest(self.esgf_index, gmetalist)
        task_id = r.get("task_id")
        print("Ingested successfully, waiting for task to complete...")

        while True:
            r = self.search_client.get_task(task_id)
            state = r.get("state")
            if state == "SUCCESS":
                return True
            if state == "FAILED":
                logging.error(f"Ingestion task {task_id} failed")
                logging.error(r.text)
                return False
            time.sleep(1)
        return True
