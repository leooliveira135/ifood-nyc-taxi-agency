import boto3
import logging
import time
from botocore.exceptions import ClientError
from ifood.vars import aws_glue_role

def create_glue_database(database_name: str, aws_region: str) -> None:
    """
        Create a Glue database if it does not exist.
        Args:
            database_name (str): The name of the Glue database to create.
            aws_region (str): The AWS region where the database will be created.
        Returns:
            None
    """
    glue = boto3.client('glue', region_name=aws_region)

    try:
        if database_name in [db['Name'] for db in glue.get_databases()['DatabaseList']]:
            logging.info(f"Glue database {database_name} already exists.")
            return
        
        logging.info(f"Creating Glue database: {database_name}")
        glue.create_database(
            DatabaseInput={
                'Name': database_name,
                'Description': f'Database for {database_name} athena schema.'
            }
        )
        logging.info(f"Glue database {database_name} created successfully.")
    except Exception as e:
        logging.error(f"Failed to create Glue database {database_name}: {e}")
        raise

def create_glue_crawler(crawler_name: str, database_name: str, s3_path: str, aws_region: str, account_id: str) -> None:
    """
        Create a Glue crawler to catalog data in S3.
        Args:
            crawler_name (str): The name of the Glue crawler to create.
            database_name (str): The name of the Glue database to associate with the crawler.
            s3_path (str): The S3 path where the data is stored.
            aws_region (str): The AWS region where the crawler will be created.
            account_id (str): The AWS account ID.
        Returns:
            None
    """
    glue = boto3.client('glue', region_name=aws_region)
    role_arn = f"arn:aws:iam::{account_id}:role/{aws_glue_role}"

    try:
        logging.info(f"Creating Glue crawler: {crawler_name}")
        glue.create_crawler(
            Name=crawler_name,
            Role=role_arn,
            DatabaseName=database_name,
            Targets={
                'S3Targets': [
                    {'Path': s3_path.replace("s3a://", "s3://")},
                    {'Exclusions': ['**/_delta_log/**']}
                ]
            }
        )
        logging.info(f"Glue crawler {crawler_name} created successfully.")

    except ClientError as e:
        logging.error(f"Error creating crawler {crawler_name}: {e}")

def start_glue_crawler(crawler_name: str, database_name: str) -> None:
    """
        Start a Glue crawler to catalog data in S3.
        Args:
            crawler_name (str): The name of the Glue crawler to start.
            database_name (str): The name of the Glue database associated with the crawler.
        Returns:
            None
    """
    glue = boto3.client('glue')
    response = glue.start_crawler(Name=crawler_name)

    logging.info(f"Starting Glue crawler: {crawler_name}")
    logging.info(f"Status of crawler {crawler_name}: {response}")

    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
        logging.error(f"Failed to start crawler '{crawler_name}'")
        exit(1)

    while True:
        state = glue.get_crawler(Name=crawler_name)['Crawler']['State']
        logging.info(f"Crawler {crawler_name} current state: {state}")
        if state == "READY":
            break
        time.sleep(10)

    logging.info(f"Glue crawler {crawler_name} finished successfully.")

    db_list = glue.get_tables(DatabaseName=database_name)
    table_list = [table['Name'] for table in db_list['TableList']]
    logging.info(f"Tables in database {database_name}: {table_list}")
