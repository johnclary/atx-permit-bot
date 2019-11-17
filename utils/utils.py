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


def parse_folder_details(soup, data):

    permit_content = soup.find_all("div", {"class": "group"})

    permit_content = soup.find_all("div", {"class": "group"})

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


def parse_property_details(soup, data):

    property_headers = [
        "property_number",
        "property_pre",
        "property_street",
        "property_streettype",
        "property_dir",
        "property_unit_type",
        "property_unit_number",
        "property_city",
        "property_state",
        "property_zip",
        "property_legal_desc",
    ]

    tables = soup.find_all("table")

    # we expect property details to be the 2nd table
    propert_info_table = tables[1]

    table_rows = propert_info_table.find_all("tr")

    for tr in table_rows:
        td = tr.find_all("td")
        row = [i.text.strip() for i in td]

        # sometimes the property details has a street segment entry, which has different columns from an address entry
        # we only want address entries. we veryify by checking number of cells in row
        if len(row) == 11:
            data.update(dict(zip(property_headers, row)))

            # a permit could have multiple properties. we just take the first.
            break

    return data


def parse_html(html):
    """
    Extract permit data from html and returnt contents as dict.
    Also add a "scrape_status" value, meant to flag the status of
    the rsn record in postgrest.
    """
    soup = BeautifulSoup(html, "html.parser")

    data = {}

    data = parse_folder_details(soup, data)

    data = parse_property_details(soup, data)

    return data
