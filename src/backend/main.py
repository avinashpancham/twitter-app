import logging
from tweepy_utils import start_stream

if __name__ == "__main__":
    logging.warning("Backend started")
    start_stream()
