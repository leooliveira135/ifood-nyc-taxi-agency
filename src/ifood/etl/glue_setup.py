import logging
from ifood.aws.glue_catalog import create_glue_database, create_glue_crawler, start_glue_crawler
from ifood.vars import glue_database, glue_database_stg, s3_stg_bucket, s3_bucket

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

def run_glue_catalog(aws_credentials: dict):
    """
        Setup AWS Glue catalog with databases and crawlers.
        Args:
            aws_credentials (dict): Dictionary containing AWS credentials with keys 'account_id' and 'region'.
        Returns:
            None
    """
    logging.info("Setting up AWS Glue catalog...")
    account_id = aws_credentials['account_id']
    aws_region = aws_credentials['region']
    glue_setup(aws_region, account_id)
    logging.info("AWS Glue catalog setup completed successfully.\n")

