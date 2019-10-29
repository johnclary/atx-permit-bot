import pdb
import logging
from logging.handlers import RotatingFileHandler
import time

import requests
import twitter

from config.secrets import (
    CONSUMER_API_KEY,
    CONSUMER_API_SECRET,
    TWITTER_ACCESS_TOKEN,
    TWITTER_ACCESS_TOKEN_SECRET,
    ENDPOINT,
    TOKEN
)
from config.config import BASE_URL

"""
TODO:
- truncate long message to preserve URL
- update postgrest after tweet
- handle issued tweets
- space out tweets
"""

def update_permit(rsn):

    headers = {
        "Authorization" : f"Bearer {TOKEN}",
        "Content-Type" : "application/json",
        "Prefer": "resolution=merge-duplicates"
    }

    params = {
        "rsn" : f"eq.{rsn}"
    }

    data = {
        "bot_status" : "posted"
     }

    return requests.patch(ENDPOINT, headers=headers, json=data, params=params)


def parse_subtype(subtype):
    # e.g. etxtract subtype description from something like `R- 101 Single Family Houses`
    subtype = subtype.replace('- ', '')
    subtype = subtype.split(' ')
    return ' '.join(subtype[1:])


def init_api():
    return twitter.Api(
        consumer_key=CONSUMER_API_KEY,
        consumer_secret=CONSUMER_API_SECRET,
        access_token_key=TWITTER_ACCESS_TOKEN,
        access_token_secret=TWITTER_ACCESS_TOKEN_SECRET,
    )


def format_tweet(permit):
    return f"{permit['status']}: {permit['subtype']} at {permit['project_name']}. {BASE_URL}{permit['rsn']}"


def get_params(tweet_type=None):

    if tweet_type == "new_applcation":
        return {
            "bot_status": "eq.not_posted",
            "application_date": "not.is.null",
            "permit_id": "like.*BP*",
            "select": "project_name,permit_id,subtype,rsn,status",
        }

    elif tweet_type == "issued":
        return {
            "bot_status": "eq.not_posted",
            "issued": "not.is.null",
            "permit_id": "like.*BP*",
        }

    else:
        raise Exception("tweet_type must be `new_applcation` or `issued`")


def get_data(url, params):
    res = requests.get(url, params=params)
    res.raise_for_status()
    return res.json()


def main():
    api = init_api()

    params = get_params(tweet_type="new_applcation")

    data = get_data(ENDPOINT, params)

    pdb.set_trace()
    for permit in data:
        subtype = permit["subtype"]
        permit["subtype"] = parse_subtype(subtype)
        
        tweet = format_tweet(permit)
        logger.info(tweet)

        res = api.PostUpdate(tweet)
        res = 1
        if (res):
            res = update_permit(permit["rsn"])
            res.raise_for_status()

        else:
            raise Exception("Unknown error posting tweet.")

        time.sleep(5)

"""
case 1: new applications
- where:
- twitter has not been posted
- application date is populated
- issued is not populated
- permit type contains xyz

1. get data
2. post tweet
3. update bot status status

case 2: issued permit
- where:
- twitter has not been posted
- application and issued are populated
- permit type contains xyz
"""

if __name__ == "__main__":
    logger = logging.getLogger("my_logger")
    logging.basicConfig(
        format="%(asctime)s %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p"
    )
    handler = RotatingFileHandler("log/twitter-bot.log", maxBytes=20000000)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    main()
