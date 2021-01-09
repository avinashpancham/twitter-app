class ElasticsearchRetriever:
    def __init__(self, es):
        self.es = es

    def search(self, location):
        response = self.es.search(
            index="tweets",
            body={"size": 100, "query": {"match": {"location_name": location}}},
        )
        return self.parse_response(response)

    @staticmethod
    def parse_response(response):
        return [
            {
                "text": tweet["_source"]["text"],
                "user": tweet["_source"]["user_name"],
                "created_at": tweet["_source"]["created_at"],
            }
            for tweet in response["hits"]["hits"]
        ]
