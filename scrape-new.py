"""
Find and download new permits from the City of Austin's public search page.
"""
import argparse
from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler
from multiprocessing import Pool
import pdb

import requests

from config.config import BASE_URL, DATESTRING_FORMAT, FIELDMAP, DATE_FIELDS
from config.secrets import ENDPOINT, TOKEN
import utils


def cli_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-b",
        "--backdate",
        type=int,
        required=True,
        help="The number of historical not-found RSNs to crawl."
    )

    parser.add_argument(
        "-n",
        "--new",
        type=int,
        required=True,
        help="The number of new, not yet searched RSNs to attempt to find before giving up."
    )

    args = parser.parse_args()
    return args


def load(data):

    headers = {
        "Authorization" : f"Bearer {TOKEN}",
        "Content-Type" : "application/json",
        "Prefer": "resolution=merge-duplicates"
    }

    return requests.post(ENDPOINT, headers=headers, json=data)



def success(html):
    # make sure the html contains permit data
    if "No Rows Returned" in html:
        return False
    else:
        return True


def get_permit_html(rsn):

    now = datetime.now().strftime(DATESTRING_FORMAT)

    url = f"{BASE_URL}{rsn}"

    print(url)

    res = requests.get(url)

    res.raise_for_status()

    if not success(res.text):
        return None

    else:
        return res.text


def get_not_found_rsns(limit):
    """
    Get the largest RSN from the database. RSNs uniqule identify permits, and they are sequential.
    So the largest RSN is also the newest.
    """
    params = {
        "select" : "rsn",
        "order" : "rsn.desc",
        "limit" : limit,
        "scrape_status" : "eq.not_found",
    }

    res = requests.get(ENDPOINT, params=params)

    return res.json()


def main():

    args = cli_args()

    rsn_backdate = args.backdate # how many "not_found" rsns to go back to check to see if they exist

    max_useless_search = args.new # how many searches to perform for future RSNs before giving up

    not_found_rsns = get_not_found_rsns(rsn_backdate) # all rsns (limit = rsn_backdate) that have no permit data

    search_rsns = [int(rsn["rsn"]) for rsn in not_found_rsns] # listify not found rsns to search

    search_rsns.reverse()

    max_rsn = search_rsns[-1] # the current largest RSN that has ever been searched for and not found
 
    print(f"MAX RSN: {max_rsn}")

    global_search_count = 0 # how many searches have been attempted
    
    no_results_count = 0 # how many searches have been performed without finding an RSN

    current_rsn = search_rsns[0]

    while (current_rsn <= max_rsn) or (no_results_count < max_useless_search):
        """
        Search for RSNs by incrementing forward through all backlog RSNs, as
        Well as forward past the latest known RSN, up to the max_useless_search amount.
        If nothing found new found, give up.
        If something found, continue searching until nothing found.
        Update status of in-between permits to something
        """
        if global_search_count < len(search_rsns):
            # search through historical RSNs, known to be previously not found
            current_rsn = search_rsns[global_search_count]
            
            no_results_count = 0

        # search through any future RSNs
        current_rsn += 1

        print(f"CURRENT: {current_rsn}")

        no_results_count += 1

        global_search_count += 1

        now = datetime.now().strftime(DATESTRING_FORMAT)

        data = {
            "rsn" : str(current_rsn),
            "scrape_status" : "not_found",
            "scrape_date" : now
        }

        html = get_permit_html(current_rsn)

        if html:
            # reset search count, keeping searching into future
            print(f"FOUND {current_rsn}")
            
            parsed_html = utils.parse_html(html)
        
            if parsed_html:
                # update payload with parse permit attributes and scrape status

                data.update(parsed_html)

                data = utils.replace_keys(data, FIELDMAP)

                data = utils.handle_dates(data, DATE_FIELDS)

                # reset the search countdown
                no_results_count = 0

            
        res = load(data)

        res.raise_for_status()

        continue


if __name__ == "__main__":
    logger = logging.getLogger("my_logger")
    logging.basicConfig(
        format="%(asctime)s %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p"
    )
    handler = RotatingFileHandler("log/scrape-new.log", maxBytes=20000000)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    main()