import logging
import time

from globus_sdk import AccessTokenAuthorizer, ConfidentialAppAuthClient, SearchClient
from globus_sdk.scopes import SearchScopes

from settings import globus_search, globus_search_client_credentials


class ConsumerSearchClient:
    def __init__(self):
        confidential_client = ConfidentialAppAuthClient(
            client_id=globus_search_client_credentials.get("client_id"),
            client_secret=globus_search_client_credentials.get("client_secret"),
        )
        token_response = confidential_client.oauth2_client_credentials_tokens(
            requested_scopes=SearchScopes.all,
        )
        search_tokens = token_response.by_resource_server.get("search.api.globus.org")
        authorizer = AccessTokenAuthorizer(search_tokens.get("access_token"))
        self.search_client = SearchClient(authorizer=authorizer)
        self.esgf_index = globus_search.get("index")

    def convert_assets(self, item):
        converted_assets = []
        for key, value in item.get("assets").items():
            converted_assets.append({"name": key} | value)
        return converted_assets

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
            assets = item.get("assets")
            assets_list = []
            for name, asset in assets.items():
                asset["name"] = name
                assets_list.append(asset)
            item["assets"] = assets_list
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
                return False
            time.sleep(1)
        return True

    def delete(self, subject):
        self.search_client.delete_subject(self.esgf_index, subject)
