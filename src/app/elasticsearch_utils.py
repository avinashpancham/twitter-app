from typing import List

from elasticsearch import Elasticsearch

from _typing import JsonType


class ElasticsearchRetriever:
    def __init__(self, es: Elasticsearch) -> None:
        self.es = es

    def search(self, location: str) -> JsonType:
        response = self.es.search(
            index="tweets",
            body={"size": 100, "query": {"match": {"location_name": location}}},
        )
        return {"response": self.parse_response(response)}

    @staticmethod
    def parse_response(response: JsonType) -> List[JsonType]:
        return [
            {
                "text": tweet["_source"]["text"],
                "user": tweet["_source"]["user_name"],
                "created_at": tweet["_source"]["created_at"],
            }
            for tweet in response["hits"]["hits"]
        ]
