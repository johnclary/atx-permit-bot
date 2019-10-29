BASE_URL = "https://abc.austintexas.gov/web/permit/public-search-other?t_detail=1&t_selected_folderrsn="

DATESTRING_FORMAT = "%Y-%m-%dT%H:%M:%S"

FIELDMAP = {
    'Permit/Case' : "permit_id",
    'Project Name' : "project_name",
    'Application Date' : "application_date",
    'Description' : "description",
    'Status' : "status",
    'Reference File Name' : "reference_file_name",
    'Sub Type' : "subtype",
    'Work Type' : "worktype",
    'Related Folder' : "related_folder",
    'Expiration Date' : "expiration_date",
    'Issued' : "issued"
}

DATE_FIELDS = ["application_date", "issued", "expiration_date"]

TWEET_SERVER = "http://0.0.0.0:8000/tweet"