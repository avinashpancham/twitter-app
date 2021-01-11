import logging
from datetime import datetime
from typing import List

import spacy
from langdetect import detect
from _typing import JsonType
from elasticsearch import Elasticsearch

from stopwords import dutch_english_stop_words

ALLOWABLE_SPACY_LABELS = ("ORG", "LOC", "EVENT", "NORP", "FAC", "GPE", "PERSON")


def process_and_store_data(data: JsonType) -> None:
    topics = extract_topics(data["text"])
    store_data(data, topics)


def extract_topics(text: str) -> List[str]:
    # Twitter language info often not available
    dutch = detect(text) == "nl"
    nlp_model = spacy.load("nl_core_news_sm") if dutch else spacy.load("en_core_web_sm")

    doc = nlp_model(text.replace("#", ""))
    return [
        ent.text.lower()
        for ent in doc.ents
        if ent.label_ in ALLOWABLE_SPACY_LABELS
        and ent.text.lower() not in dutch_english_stop_words
        and ent.text.isalnum()
    ]


def store_data(data: JsonType, topics: List[str]) -> None:
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
        "topics": topics,
    }

    es.index(index="tweets", body=es_data)
    logging.info(es_data)


def local_testing() -> None:
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
                "topics": ["Test"],
            },
        )

    mapping = es.indices.get_mapping(index="tweets")
    print(mapping)

    response = es.search(index="tweets", body={"query": {"match_all": {}}})
    print(response)


if __name__ == "__main__":
    local_testing()
