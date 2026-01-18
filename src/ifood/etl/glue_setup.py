import logging
import time
from ifood.api.zip_folder import zip_folder, unzip_file
from ifood.aws.glue_catalog import create_glue_database, create_glue_crawler, start_glue_crawler, list_glue_db_tables, create_glue_job, run_glue_job, wait_for_glue_job_completion
from ifood.aws.s3_bucket import upload_file_s3_bucket
from ifood.vars import glue_database, glue_database_stg, s3_stg_bucket, glue_iceberg_job, glue_job_path, iceberg_bucket, glue_iceberg_job_path, glue_zip_path

def glue_setup(aws_region: str, account_id: str) -> None:
    """
        Setup AWS Glue database and crawler.
        Args:
            aws_region (str): The AWS region.
            account_id (str): The AWS account ID.
        Returns:
            None
    """
    logging.info("Setting up AWS Glue database...")
    logging.info(f"Creating Glue crawler for staging database {glue_database_stg} in {aws_region}...")
    create_glue_database(glue_database_stg, aws_region)
    logging.info("AWS Glue databases created successfully.")
    crawler_name_stg = f"{glue_database_stg}_crawler"
    s3_path_stg = f"s3://{s3_stg_bucket}/"
    logging.info("Creating AWS Glue crawlers...")
    create_glue_crawler(crawler_name_stg, glue_database_stg, s3_path_stg, aws_region, account_id)
    logging.info("AWS Glue crawlers created successfully.")
    logging.info("Starting AWS Glue crawlers...")
    start_glue_crawler(crawler_name_stg, aws_region)
    logging.info("AWS Glue crawlers started and completed successfully.")

def iceberg_setup(aws_region: str, account_id: str) -> None:
    """
        Setup AWS Glue database and crawler.
        Args:
            aws_region (str): The AWS region.
            account_id (str): The AWS account ID.
        Returns:
            None
    """
    logging.info("Setting up AWS Glue job for Iceberg tables...")
    bucket_name = glue_job_path.split('/')[0]
    bucket_key = glue_job_path.split('/')[1]
    zip_folder("src/ifood", "ifood_libs.zip")
    unzip_file("ifood_libs.zip", "/tmp")
    logging.info(f"Uploading {glue_iceberg_job_path} file into {bucket_name}")
    glue_script = upload_file_s3_bucket(bucket_name, bucket_key, glue_iceberg_job_path, aws_region)
    logging.info(f"Glue script path in S3 bucket: {glue_script}")
    logging.info(f"Uploading ifood_libs.zip file into {bucket_name}")
    glue_zip = upload_file_s3_bucket(bucket_name, bucket_key, glue_zip_path, aws_region)
    logging.info(f"Glue zip file path in S3 bucket: {glue_zip}")
    logging.info("AWS Glue Job created successfully.")
    table_list = list_glue_db_tables(glue_database_stg, aws_region)
    for table in table_list:
        source_path = f"s3://{s3_stg_bucket}/{table}"
        iceberg_path = f"{iceberg_bucket}/{table}"
        glue_table_job = f"{glue_iceberg_job}-{table}"
        logging.info(f"Creating Glue Job for Iceberg tables {glue_table_job}")
        create_glue_job(
            job_name=glue_table_job, 
            account_id=account_id, 
            aws_region=aws_region, 
            glue_job_path=glue_script,
            extra_py_files="s3://ifood-nyc-taxi-agency/scripts/ifood_libs.zip",
            timeout=10
        )
        logging.info(f"Starting AWS Glue Job for the Iceberg table {table}...")
        job_arguments = {
            "--SOURCE_DATABASE": glue_database_stg,
            "--TABLE_NAME": table,
            "--TARGET_DATABASE": glue_database,
            "--ICEBERG_LOCATION": iceberg_path,
            "--SOURCE_PATH": source_path,
        }
        job_run_id = run_glue_job(glue_table_job, job_arguments, aws_region)
        job_status = wait_for_glue_job_completion(glue_table_job, job_run_id, aws_region, 20, 3600)
        logging.info(f"AWS Glue Job for the Iceberg table {table} completed with status {job_status}...")

def run_glue_catalog(aws_credentials: dict):
    """
        Setup AWS Glue catalog with databases and crawlers.
        Args:
            aws_credentials (dict): Dictionary containing AWS credentials with keys 'account_id' and 'region'.
        Returns:
            None
    """
    account_id = aws_credentials['account_id']
    aws_region = aws_credentials['region']
    logging.info("Setting up AWS Glue catalog...")
    glue_setup(aws_region, account_id)
    logging.info("AWS Glue catalog setup completed successfully.\n")
    logging.info("Setting up AWS Glue Job...")
    iceberg_setup(aws_region, account_id)
    logging.info("AWS Glue Job setup completed successfully.\n")

