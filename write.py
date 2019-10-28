"""
Catpture raw permit html and write to file.

```bash
sudo docker run --name all-the-permits --rm -d -v /home/ec2-user/atx-permit-bot:/app -w /app atx-permit-bot python write.py

sudo docker run --name all-the-permits --rm -it -v /home/ec2-user/atx-permit-bot:/app -w /app atx-permit-bot python write.py
```
"""
from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler
from multiprocessing import Pool
import os
import pdb

import requests

from config.config import BASE_URL, DATESTRING_FORMAT
from config.secrets import ENDPOINT, TOKEN


def success(html):
    # make sure the html contains permit data
    if "No Rows Returned" in html or not html:
        return False
    else:
        return True


def async_get_permits(rsn):
    now = datetime.now().strftime(DATESTRING_FORMAT)
    print(now)
    print(rsn)
    url = f"{BASE_URL}{rsn}"

    html = get_permit(url)

    logger.info(f"RSN: {rsn}")

    if not success(html):
        fname = f"s3/{rsn}_NO_DATA.html"

    else:
        fname = f"s3/{rsn}.html"

    with open(fname, "w") as fout:
        print("write")
        fout.write(html)

    return None


def get_permit(url):

    success = False

    while not success:
        
        try:
            print(f"trying: {url}")
            res = requests.get(url)
            print("got a response")
            res.raise_for_status()
            success = True

        except Exception as e:
            print("error")
            logging.error(res.text)
            return ""

    return res.text


def get_unscraped_rsns(max_rsn, scraped_rsns):
    print("getting unscraped rsns")
    unscraped_rsns = []
    
    if not scraped_rsns:
        scraped_rsns = [10000000]

    for rsn in range(min(scraped_rsns), max_rsn):
        if rsn not in scraped_rsns:
            unscraped_rsns.append(rsn)

    return unscraped_rsns


def get_scraped_rsns(path):
    print("getting scraped rsns")
    rsns = []
    for file in os.scandir(path):
        if ".html" in file.path:
            fname = file.path.replace(".html", "").replace("s3/", "")
            rsn = int(fname.split("_")[0])
            rsns.append(rsn)

    return rsns


def main():
    max_rsn = 12353184 # the largest (most recent) RSN that we want to scrape
    print("starting")

    scraped_rsns =  get_scraped_rsns("s3")
    
    unscraped_rsns = get_unscraped_rsns(max_rsn, scraped_rsns)

    with Pool(processes=4) as pool:
        pool.map(async_get_permits, unscraped_rsns)

    logger.info("done")
    

if __name__ == "__main__":
    logger = logging.getLogger("my_logger")
    logging.basicConfig(
        format="%(asctime)s %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p"
    )
    handler = RotatingFileHandler("log/write.log", maxBytes=200000000)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    main()
