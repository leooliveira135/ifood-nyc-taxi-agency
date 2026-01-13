from pathlib import Path

project_root = Path(__file__).resolve().parents[1]

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
s3_stg_bucket = "ifood-nyc-taxi-agency-stg/delta"
s3_bucket = "ifood-nyc-taxi-agency/delta"

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
    "total_amount",
    "ingestion_date",
    "data_source",
    "source_date"
]

glue_database_stg = "ifood_nyc_taxi_agency_stg"
glue_database = "ifood_nyc_taxi_agency"
aws_glue_role = "glue-crawler-role"

athena_output_queries = "s3://ifood-nyc-taxi-agency/athena/"
iceberg_bucket = "ifood-nyc-taxi-agency/iceberg"

glue_iceberg_job = "ifood-glue-iceberg"
glue_job_path = "ifood-nyc-taxi-agency/scripts"
glue_iceberg_job_path = (project_root / "aws" / "glue_iceberg_job.py")
