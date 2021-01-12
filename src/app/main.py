from elasticsearch import Elasticsearch
from fastapi import FastAPI

from _typing import JsonType
from elasticsearch_utils import ElasticsearchProcessor, location_72h_query
from models import Text

app = FastAPI()


@app.get("/")
def root() -> JsonType:
    return {"Twitter": "Streamer"}


@app.post("/trending/")
async def get_location_72h_trending(text: Text) -> JsonType:
    ep = ElasticsearchProcessor(
        Elasticsearch([{"host": "elasticsearch-1-vm", "port": 9200}]),
        location_72h_query(text.location),
    )
    ep.search()
    return ep.analyze_trends()
