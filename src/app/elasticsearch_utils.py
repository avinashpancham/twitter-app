import itertools
from collections import Counter

from _typing import JsonType
from elasticsearch import Elasticsearch


class ElasticsearchProcessor:
    def __init__(self, es: Elasticsearch, query: JsonType) -> None:
        self.es = es
        self.query = query
        self.response = {}

    def search(self) -> None:
        self.response = self.es.search(index="tweets", body=self.query)

    def analyze_trends(self) -> JsonType:
        counter = Counter(
            itertools.chain.from_iterable(
                tweet["_source"]["topics"] for tweet in self.response["hits"]["hits"]
            )
        )
        return dict(counter.most_common(n=5))


def location_72h_query(location: str) -> JsonType:
    return {
        "size": 10000,
        "query": {
            "bool": {
                "must": [
                    {"match": {"location_name": location}},
                    {"range": {"created_at": {"gte": "now-72h"}}},
                ]
            }
        },
    }
