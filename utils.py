from datetime import datetime
from bs4 import BeautifulSoup


def handle_dates(d, fields):
    """
    Reformat datestrings.
    In format: Oct 21, 2019
    Out format: YYYY-MM-DD
    """
    for key in d.keys():
        if key in fields:
            if d[key]:
                d[key] = datetime.strptime(d[key], "%b %d, %Y").strftime("%Y-%m-%d")
            else:
                d[key] = None

    return d


def replace_keys(d, fields):
    """
    Map humanized html fieldnames to db columns
    """
    new_dict = {}

    for key in d.keys():
        if key in fields:
            new_dict[fields[key]] = d[key]
        else:
            new_dict[key] = d[key]

    return new_dict


def parse_html(html):
    """
    Extract permit data from html
    """
    soup = BeautifulSoup(html, "html.parser")

    permit_content = soup.find_all("div", {"class": "group"})

    data = {}

    if len(permit_content) > 0:

        # get the "FOLDER DETAILS" element
        permit_content[0].findChildren("span")

        # pull all the entries from this section
        spans = permit_content[0].findChildren("span")

        # drop first span, which is "FOLDER DETAILS"
        spans = spans[1:]

        # each pair of spans is a label + value
        for x in range(0, len(spans), 2):
            label = spans[x].text.strip().replace(":", "")
            val = spans[x + 1].text.strip()
            data[label] = val

        data["scrape_status"] = "captured"

    else:
        data["scrape_status"] = "retrieved_no_content"

    return data
