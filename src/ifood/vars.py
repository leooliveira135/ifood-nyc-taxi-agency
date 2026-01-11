endpoint = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"

filename_list = [
    "Yellow Taxi Trip Records",
    "Green Taxi Trip Records",
    "For-Hire Vehicle Trip Records",
    "High Volume For-Hire Vehicle Trip Records"
]

filter_year = "2023"

aws_profile_name = "default"

s3_raw_bucket = "ifood-nyc-taxi-agency-raw"
s3_stg_bucket = "ifood-nyc-taxi-agency-stg"
s3_bucket = "ifood-nyc-taxi-agency"

headers = {
    # Critical: mimic curl/browser
    "User-Agent": "curl/8.0.1",
    "Accept": "*/*",
    "Accept-Encoding": "identity",
    "Connection": "keep-alive",
}

selected_columns = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "total_amount"
]