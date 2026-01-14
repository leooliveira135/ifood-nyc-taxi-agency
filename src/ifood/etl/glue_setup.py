import logging
from ifood.aws.glue_catalog import create_glue_database, create_glue_crawler, start_glue_crawler, list_glue_db_tables, create_glue_job, run_glue_job
from ifood.aws.s3_bucket import upload_file_s3_bucket
from ifood.vars import glue_database, glue_database_stg, s3_stg_bucket, s3_bucket, glue_iceberg_job, glue_job_path, iceberg_bucket, glue_iceberg_job_path

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
    logging.info(f"Creating Glue crawler for production database {glue_database} in {aws_region}...")
    create_glue_database(glue_database, aws_region)    
    logging.info("AWS Glue databases created successfully.")
    crawler_name_stg = f"{glue_database_stg}_crawler"
    crawler_name = f"{glue_database}_crawler"
    s3_path_stg = f"s3://{s3_stg_bucket}/"
    s3_path = f"s3://{s3_bucket}/"
    logging.info("Creating AWS Glue crawlers...")
    create_glue_crawler(crawler_name_stg, glue_database_stg, s3_path_stg, aws_region, account_id)
    create_glue_crawler(crawler_name, glue_database, s3_path, aws_region, account_id)
    logging.info("AWS Glue crawlers created successfully.")
    logging.info("Starting AWS Glue crawlers...")
    start_glue_crawler(crawler_name_stg, glue_database_stg)
    start_glue_crawler(crawler_name, glue_database)
    logging.info("AWS Glue crawlers started and completed successfully.")

def iceberg_setup(account_id: str) -> None:
    """
        Setup AWS Glue job for Iceberg tables.
        Args:
            account_id (str): The AWS account ID.
        Returns:
            None
    """
    logging.info("Setting up AWS Glue job for Iceberg tables...")
    bucket_name = glue_job_path.split('/')[0]
    bucket_key = glue_job_path.split('/')[1]
    logging.info(f"Uploading {glue_iceberg_job_path} file into {bucket_name}")
    upload_file_s3_bucket(str(glue_iceberg_job_path), bucket_key, bucket_name)
    logging.info(f"Creating Glue Job for Iceberg tables {glue_iceberg_job}")
    s3_path = f"s3://{glue_job_path}/"
    create_glue_job(glue_iceberg_job, account_id, s3_path)
    logging.info("AWS Glue Job created successfully.")
    table_list = list_glue_db_tables(glue_database)
    for table in table_list:
        source_path = f"s3://{s3_bucket}/{table}"
        iceberg_path = f"s3://{iceberg_bucket}/{table}"
        logging.info(f"Starting AWS Glue Job for the Iceberg table {table}...")
        job_run_id = run_glue_job(glue_iceberg_job, glue_database, source_path, glue_database, table, iceberg_path)
        logging.info(f"AWS Glue Job for the Iceberg table {table} completed with status {job_run_id}...")


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
    iceberg_setup(account_id)
    logging.info("AWS Glue Job setup completed successfully.\n")

