from datetime import datetime
from elasticsearch import Elasticsearch


if __name__ == "__main__":
    es = Elasticsearch()
    if not es.indices.exists(index="tweets"):
        es.indices.create(index="tweets", ignore=400)

        # Add record to initialize mapping
        es.index(
            index="tweets",
            body={
                "country": "NL",
                "created_at": datetime(2021, 1, 9, 0, 0, 0),
                "hashtag": [],
                "id": 1,
                "lang": "nl",
                "location_name": "Test",
                "text": "Test message",
                "user_id": 1,
                "user_name": "Test",
            },
        )

    mapping = es.indices.get_mapping(index="tweets")
    print(mapping)

    response = es.search(index="tweets", body={"query": {"match_all": {}}})
    print(response)
