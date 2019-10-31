"""
Find and download new permits from the City of Austin's public search page.
"""
import argparse
from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler
import pdb

import requests

from config.config import BASE_URL, DATESTRING_FORMAT, FIELDMAP, DATE_FIELDS, TWEET_SERVER
from config.secrets import ENDPOINT, TOKEN
import utils


def cli_args():
    parser = argparse.ArgumentParser()

    # todo: these might be deprecated
    parser.add_argument(
        "-b",
        "--backdate",
        type=int,
        required=True,
        help="The number of historical not-found RSNs to crawl.",
    )

    parser.add_argument(
        "-n",
        "--new",
        type=int,
        required=True,
        help="The number of new, not yet searched RSNs to attempt to find before giving up.",
    )


    parser.add_argument(
        "-d",
        "--direction",
        type=str,
        choices=["backward", "forward"],
        required=True,
        help="The direction to search, either backward or forward",
    )

    args = parser.parse_args()
    return args


def post_tweet(data):
    return requests.post(TWEET_SERVER, json=data)


def load(data):

    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }

    return requests.post(ENDPOINT, headers=headers, json=data)


def success(html):
    # make sure the html contains permit data
    if "No Rows Returned" in html:
        return False
    else:
        return True



def process_new_permits(search_attempts):    
    search_rsn = get_latest_found_rsn()

    search_count = 0

    while search_count <= search_attempts:

            print(f"CURRENT: {search_rsn}")

            search_count += 1

            now = datetime.now().strftime(DATESTRING_FORMAT)

            data = {
                "rsn": str(search_rsn),
                "scrape_status": "not_found",
                "scrape_date": now,
                "bot_status" : "not_posted"
            }

            html = get_permit_html(search_rsn)

            if html:
                # reset search count, keeping searching into future
                print(f"FOUND {search_rsn}")

                parsed_html = utils.parse_html(html)

                if parsed_html:
                    # update payload with parse permit attributes and scrape status

                    data.update(parsed_html)

                    data = utils.replace_keys(data, FIELDMAP)

                    data = utils.handle_dates(data, DATE_FIELDS)

                    if "BP" in data["permit_id"]:
                        # post BPs to twitter

                        res = post_tweet(data)

                        res.raise_for_status()

                        data["bot_status"] = "posted"

                    # reset the search countdown
                    search_count = 0

            res = load(data)
            search_attempts += 1
            search_count += 1
            search_rsn += 1


def get_permit_html(rsn):

    now = datetime.now().strftime(DATESTRING_FORMAT)

    url = f"{BASE_URL}{rsn}"

    print(url)

    res = requests.get(url, timeout=20)

    res.raise_for_status()

    if not success(res.text):
        return None

    else:
        return res.text

def get_latest_found_rsn():
    params = {
        "select": "rsn",
        "order": "rsn.desc",
        "limit": 1,
        "scrape_status": "eq.captured",
    }

    res = requests.get(ENDPOINT, params=params)

    return int(res.json()[0]["rsn"])


def get_not_found_rsns(limit):
    """
    Get the largest RSN from the database. RSNs uniqule identify permits, and they are sequential.
    So the largest RSN is also the newest.
    """
    params = {
        "select": "rsn",
        "order": "rsn.desc",
        "limit": limit,
        "scrape_status": "eq.not_found",
    }

    res = requests.get(ENDPOINT, params=params)

    return res.json()


def main():

    args = cli_args()

    if args.direction == "forward":
        process_new_permits(args.new)
    
    


if __name__ == "__main__":
    logger = logging.getLogger("my_logger")
    logging.basicConfig(
        format="%(asctime)s %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p"
    )
    handler = RotatingFileHandler("log/scrape-new.log", maxBytes=20000000)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    main()
