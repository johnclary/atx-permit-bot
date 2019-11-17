"""
Find and download new permits from the City of Austin's public search page.

Usage:
# scape new permit RSNs until 10 consecutive RSNs are not found
python scrape.py -n 10 -d forward

# scrape most recent 1000 old not found RSNs
python scrape.py -n 1000 -d backward
"""
import argparse
from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler
import pdb

import requests

from config.config import (
    BASE_URL,
    DATESTRING_FORMAT,
    FIELDMAP,
    DATE_FIELDS,
)

from config.secrets import ENDPOINT, TOKEN
from utils import utils


def cli_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-n",
        "--number",
        required=True,
        type=int,
        help="The number of RSNs scrape. If direction is 'backward' this will be the number of not_found RSNs to search for, descending from the most recent found RSN. If direction is 'forward', this will be the number of `new` RSNs to attempt to scrape until the specified number of RSNs are not found.",
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


def tweetworthy(data):
    """
    Only tweet about some building permits (BPs), and select group of other permits:
    
    Exclude these bp permit subtypes:
    - C-1000 Commercial Remodel
    - C-1001 Commercial Finish Out
    - C- 329 Commercial structures other than buildings
    - C- 437 Addn, Alter, Convn-NonRes
    - R-435 Renovations/Remodel
    - R- 329 Res Structures Other Than Bldg
    - R- 434 Addition & Alterations

    Include these other subtypes:
    - Zoning/Rezoning
    - Film
    - Short Term Rental Type 1-A
    - Street Vendor
    - Hotel
    - Easement Release
    """
    exclude_subtypes = [
        "C-1000",
        "C-1001",
        "C- 329",
        "C- 437",
        "R- 435",
        "R- 329",
        "R- 434",
    ]

    non_bp_subtypes = [
        "Zoning/Rezoning",
        "Film",
        "Street Vendor",
        "Hotel",
        "Easement Release",
        # short term rentals handled in logic below
    ]

    if not data.get("subtype"):
        return False

    elif "BP" in data["permit_id"]:
        for subtype_prefix in exclude_subtypes:
            if subtype_prefix in data["subtype"]:
                return False
        return True

    elif "short term" in data["subtype"].lower():
        return True

    elif data["subtype"] in non_bp_subtypes:
        return True

    else:
        return False


def prep_data_payload(search_rsn):
    """
    Generate a permit DB payload.
    """
    now = datetime.now().strftime(DATESTRING_FORMAT)

    return {
        "rsn": str(search_rsn),
        "scrape_status": "not_found",
        "scrape_date": now,
        "bot_status": "nothing_to_tweet",
    }


def process_rsn(search_rsn):
    data = prep_data_payload(search_rsn)

    html = get_permit_html(search_rsn)

    if html:

        parsed_html = utils.parse_html(html)

        if parsed_html:
            # update payload with parse permit attributes and scrape status
            logger.info(f"found: {search_rsn}")

            data.update(parsed_html)

            data = utils.replace_keys(data, FIELDMAP)

            data = utils.handle_dates(data, DATE_FIELDS)

            if tweetworthy(data):
                data["bot_status"] = "ready_to_tweet"
            else:
                data["bot_status"] = "not_tweetworthy"

        else:
            logger.info(f"not found: {search_rsn}")

    return data


def process_new_permits(search_attempts):
    logger.info(f"process new permits")
    search_rsn = get_latest_found_rsn()

    search_rsn += 1

    search_count = 0

    while search_count <= search_attempts:

        print(f"CURRENT: {search_rsn}")

        search_count += 1

        data = process_rsn(search_rsn)

        res = load(data)

        if data.get("scrape_status") == "captured":
            search_count = 0
            print(f"FOUND: {search_rsn}")

        else:
            search_count += 1

        search_attempts += 1

        search_rsn += 1


def process_old_permits(backdate):
    logger.info(f"process old permits")

    not_found_rsns = get_not_found_rsns(
        backdate
    )  # all rsns (limit = rsn_backdate) that have no permit data

    search_rsns = [
        int(rsn["rsn"]) for rsn in not_found_rsns
    ]  # listify not found rsns to search

    for search_rsn in search_rsns:
        print(f"CURRENT: {search_rsn}")

        data = process_rsn(search_rsn)

        res = load(data)


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
        process_new_permits(args.number)

    elif args.direction == "backward":
        if args.number > 5000:
            raise Exception(
                "it is only possibly to query backward 5000 rsns due to server query limitations"
            )

        process_old_permits(args.number)

    else:
        raise Exception(
            "Direction not specified. Try command with `-d forward` or `-d backward`"
        )


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p"
    )
    logger = logging.getLogger("my_logger")
    handler = RotatingFileHandler("log/scrape.log", maxBytes=2000000)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    main()