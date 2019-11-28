#!/usr/bin/python3
from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler
from multiprocessing import Pool
import os
import pdb
import random
import time

import boto3
import requests

from config.config import BASE_URL, DATESTRING_FORMAT
from config.secrets import ENDPOINT_S3, TOKEN

from botocore import UNSIGNED
from botocore.config import Config


def get_last_not_found_rsn():
    """
    Get the largest RSN from the database. RSNs uniqule identify permits, and they are sequential.
    So the largest RSN is also the newest.
    """
    params = {
        "select": "rsn",
        "order": "rsn.desc",
        "limit": 1
    }

    res = requests.get(ENDPOINT_S3, params=params)

    res.raise_for_status()

    return res.json()[0]


def update_rsn(rsn, status=None):

    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json"
    }

    data = {
        "scrape_status" : status,
        "rsn" : rsn
    }

    if status == "in_progress":
        return requests.post(ENDPOINT_S3, headers=headers, json=data)
    else:
        params = {"rsn" : f"eq.{rsn}"}
        return requests.patch(ENDPOINT_S3, headers=headers, json=data, params=params)


def success(html):
    # make sure the html contains permit data
    if "No Rows Returned" in html:
        return False
    else:
        return True


def find_next_rsn():
    """
    Multiple processes may attempt to process the same RSN at the same time. We avoid this by
    attempting to insert the RSN with an "in_progress" status. If the RSN already exists,
    it has been picked up by another process, and the insert will fail. We move on to the next
    RSN.
    """
    last_rsn = get_last_not_found_rsn()

    next_rsn = last_rsn.get("rsn") + 1

    while True:
        res = update_rsn(next_rsn, status="in_progress")
        
        """
        A duplicate insertion will fail with:
        {'hint': None, 'details': 'Key (rsn)=(511001) already exists.', 'code': '23505', 'message': 'duplicate key value violates unique constraint "scrape_status_pkey"'}
        """
        if "23505" in res.text:
            next_rsn += 1
            continue
        else:
            res.raise_for_status()

        return next_rsn


def async_get_permits(rsn):
    # add 0-4 seconds between rsns intervals of .1 seconds
    r = random.randrange(0,40,1) / 10

    time.sleep(r)

    rsn = find_next_rsn()

    url = f"{BASE_URL}{rsn}"

    logger.info(f"Get {rsn}")

    html = get_permit(url)


    if not html:
        logger.info(f"Failed {rsn}")
        update_rsn(rsn, status="failed")
        return None

    logger.info(f"Found {rsn}")

    if not success(html):
        res = update_rsn(rsn, status="captured_not_found")
    else:
        res = update_rsn(rsn, status="captured")

    res.raise_for_status()

    fname = f"s3/{rsn}.html"

    with open(fname, "w") as fout:
        fout.write(html)

    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    res = s3.upload_file(fname, "permits-raw", fname)
    logging.info(f"Uploaded {rsn}")

    os.remove(fname)

    return None


def get_permit(url):
    """
    Try to get permit data until max attempts.
    Return html or nothing.
    """
    max_attempts = 4

    attempts = 0

    while  attempts < max_attempts:

        attempts += 1
        
        try:
            res = requests.get(url)
            res.raise_for_status()
            return res.text

        except Exception as e:
            logging.error(e.message)

    return None


def main():

    permits_to_process = 100000

    tasks = range(permits_to_process)

    with Pool(processes=6) as pool:
        pool.map(async_get_permits, tasks)

    logger.info("done")
    

if __name__ == "__main__":
    logger = logging.getLogger("s3_loader")
    logger.setLevel(logging.DEBUG)

    file_handler = RotatingFileHandler("log/s3.log", maxBytes=2000000)
    
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