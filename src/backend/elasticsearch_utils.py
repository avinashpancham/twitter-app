import logging
from datetime import datetime
from elasticsearch import Elasticsearch


def store_data(data):
    es = Elasticsearch([{"host": "elasticsearch-1-vm", "port": 9200}])
    es_data = {
        "id": data["id"],
        "created_at": datetime.strptime(
            data["created_at"], "%a %b %d %H:%M:%S +0000 %Y"
        ),
        "text": data["text"],
        "hashtag": data["entities"]["hashtags"],
        "user_id": data["user"]["id"],
        "user_name": data["user"]["name"],
        "lang": data["lang"],
        "country": "NL",
        "location_name": data["place"]["name"]
        if data["place"]
        else data["user"]["location"].split(",")[0].strip(),
    }

    es.index(index="tweets", body=es_data)
    logging.info(es_data)


if __name__ == "__main__":
    # For local testing
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
