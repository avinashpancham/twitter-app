from elasticsearch import Elasticsearch
from fastapi import FastAPI

from _typing import JsonType
from elasticsearch_utils import ElasticsearchRetriever
from models import Text

app = FastAPI()


@app.get("/")
def root() -> JsonType:
    return {"Twitter": "Streamer"}


@app.post("/trending/")
async def get_trending(text: Text) -> JsonType:
    er = ElasticsearchRetriever(
        Elasticsearch([{"host": "elasticsearch-1-vm", "port": 9200}])
    )
    return er.search(text.location)
