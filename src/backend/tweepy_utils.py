import json
import os
from datetime import datetime

from tweepy import API, OAuthHandler, Stream, StreamListener
from elasticsearch import Elasticsearch

es = Elasticsearch()


class CustomStreamListener(StreamListener):
    @staticmethod
    def store_data(data):
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
        print(es_data)

    @staticmethod
    def in_netherlands(data):
        return (data["place"] and data["place"]["country_code"] == "NL") or (
            data["user"]["location"]
            and (
                "netherlands" in data["user"]["location"].lower()
                or "nederland" in data["user"]["location"].lower()
            )
        )

    def on_data(self, data):
        data = json.loads(data)
        if "delete" not in data and self.in_netherlands(data):
            self.store_data(data)

        return True

    def on_error(self, status):
        return True


class TwitterStreamer:
    def __init__(self, listener, auth):
        self.listener = listener
        self.auth = auth
        self.api = API(
            self.auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True
        )
        self.stream = Stream(
            auth=API(
                self.auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True
            ).auth,
            listener=self.listener,
        )

    def sample(self):
        return self.stream.sample()


class Credentials:
    def __init__(self):
        self.auth = OAuthHandler(
            os.environ["CONSUMER_KEY"], os.environ["CONSUMER_SECRET"]
        )
        self.auth.set_access_token(
            os.environ["ACCESS_TOKEN"], os.environ["ACCESS_TOKEN_SECRET"]
        )


def start_stream():
    listener = CustomStreamListener()
    auth = Credentials()
    streamer = TwitterStreamer(listener, auth.auth)
    streamer.sample()
