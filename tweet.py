"""
Tweet

todo; test this; write docker command

"""
import logging
from logging.handlers import RotatingFileHandler
import pdb
import time

import requests
import twitter

from config.config import BASE_URL

from config.secrets import (
    CONSUMER_API_KEY,
    CONSUMER_API_SECRET,
    TWITTER_ACCESS_TOKEN,
    TWITTER_ACCESS_TOKEN_SECRET,
    ENDPOINT,
    TOKEN,
)

from utils import utils


def load(data):

    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }

    return requests.post(ENDPOINT, headers=headers, json=data)


def get_tweet_data():

    params = {"order": "rsn.desc", "bot_status": "eq.ready_to_tweet"}

    res = requests.get(ENDPOINT, params=params)

    res.raise_for_status()

    return res.json()


def parse_subtype(subtype):
    # e.g. etxtract subtype description from something like `R- 101 Single Family Houses`
    if "R-" in subtype or "C-" in subtype:
        subtype = subtype.replace("- ", "")
        subtype = subtype.split(" ")
        return " ".join(subtype[1:])
    else:
        return subtype


def format_tweet(permit):
    if not permit.get("property_zip"):
        return (
            f"{permit['subtype']} at {permit['project_name']} {BASE_URL}{permit['rsn']}"
        )

    else:
        return f"{permit['subtype']} at {permit['project_name']} ({permit['property_zip']}) {BASE_URL}{permit['rsn']}"


def main():

    data = get_tweet_data()

    if not data:
        logger.info("nothing to tweet")

    # instantiate the api on every new data pull
    api = twitter.Api(
        consumer_key=CONSUMER_API_KEY,
        consumer_secret=CONSUMER_API_SECRET,
        access_token_key=TWITTER_ACCESS_TOKEN,
        access_token_secret=TWITTER_ACCESS_TOKEN_SECRET,
    )

    for permit in data:

        subtype = permit["subtype"]

        permit["subtype"] = parse_subtype(subtype)

        tweet = format_tweet(permit)

        logger.info(tweet)

        try:
            res = api.PostUpdate(tweet)

        except Exception as e:
            # handle when twitter rejects a duplicate tweet
            # this happens occasionally for duplicate permit types at the same `project_name`
            # twitter api error is [{'code': 187, 'message': 'Status is a duplicate.'}]
            for message in e.message:
                if message.get("code") != 187:
                    raise e
            pass

        update_payload = {"bot_status": "tweeted", "rsn": permit["rsn"]}

        res = load(update_payload)

        res.raise_for_status()

        # sleep for a bit between tweets
        time.sleep(3)


if __name__ == "__main__":
    logger = logging.getLogger("tweet_logger")
    logger.setLevel(logging.DEBUG)

    file_handler = RotatingFileHandler("log/tweet.log", maxBytes=2000000)
    
    formatter = logging.Formatter(
        fmt="%(asctime)s %(message)s",
        datefmt="%m/%d/%Y %I:%M:%S %p",
    )

    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)

    console = logging.StreamHandler()

    console.setFormatter(formatter)

    logger.addHandler(console)
        
    main()
