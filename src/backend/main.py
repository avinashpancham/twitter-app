import logging

from tweepy_utils import start_stream

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info("Backend started")
    start_stream()
