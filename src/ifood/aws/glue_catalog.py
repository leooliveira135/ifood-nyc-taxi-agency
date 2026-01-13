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
                    {
                        'Path': s3_path.replace("s3a://", "s3://"),
                        'Exclusions': ['**/_delta_log/**']
                    }
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

def list_glue_db_tables(database_name):
    """
        List all tables in a Glue database.
        Args:
            database_name (str): The name of the Glue database.
        Returns:
            list: A list of table names in the database.
    """
    glue = boto3.client('glue')
    db_list = glue.get_tables(DatabaseName=database_name)
    table_list = [table['Name'] for table in db_list['TableList']]
    logging.info(f"Tables in database {database_name}: {table_list}")
    return table_list

def create_glue_job(job_name:str, account_id: str, glue_job_path: str, glue_version: str="4.0", worker_type: str="G.1X", num_workers: int=5, timeout: int=60):
    """
        Create a Glue job for ETL processing.
        Args:
            job_name (str): The name of the Glue job to create.
            account_id (str): The AWS account ID.
            glue_job_path (str): The S3 path to the Glue job script.
            glue_version (str): The Glue version to use. Defaults to "4.0".
            worker_type (str): The type of worker to use. Defaults to "G.1X".
            num_workers (int): The number of workers to allocate. Defaults to 5.
            timeout (int): The job timeout in minutes. Defaults to 60.
        Returns:
            None
    """

    glue = boto3.client('glue')
    role_arn = f"arn:aws:iam::{account_id}:role/{aws_glue_role}"

    try:
        try:
            glue.get_job(JobName=job_name)
            logging.error(f"Glue job {job_name} already exists — skipping creation")
            exit(1)

        except glue.exceptions.EntityNotFoundException:
            logging.info(f"Glue job {job_name} not found — creating it")
        
        glue.create_job(
            Name=job_name,
            Role=role_arn,
            ExecutionProperty={"MaxConcurrentRuns": 1},
            Command={
                "Name": "glueetl",
                "ScriptLocation": glue_job_path,
                "PythonVersion": "3"
            },
            DefaultArguments={
                "--job-language": "python",
                "--enable-glue-datacatalog": "true",
                "--datalake-formats": "delta,iceberg",
                "--enable-metrics": "true",
                "--enable-continuous-cloudwatch-log": "true"
            },
            GlueVersion=glue_version,
            WorkerType=worker_type,
            NumberOfWorkers=num_workers,
            Timeout=timeout
        )

        logging.info(f"Glue job {job_name} created successfully")

    except ClientError as e:
        logging.error(f"AWS ClientError while creating Glue job {job_name}")

    except Exception as e:
        logging.error(f"Unexpected error while creating Glue job {job_name}")

def run_glue_job(job_name:str, glue_catalog: str, source_path: str, target_db: str, target_table:str, iceberg_location:str):
    """
        Run a Glue job for ETL processing.
        Args:
            job_name (str): The name of the Glue job to run.
            glue_catalog (str): The Glue catalog name to use.
            source_path (str): The S3 path to the source data.
            target_db (str): The target Glue database name.
            target_table (str): The target table name.
            iceberg_location (str): The S3 location for Iceberg table data.
        Returns:
            str: The job run ID.
    """

    glue = boto3.client('glue')

    try:
        logging.info(f"Starting Glue job: {job_name}")
        logging.info(f"""Job arguments\n 
                     Source path: {source_path}\n 
                     Glue Catalog: {glue_catalog}\n 
                     Target database: {target_db}\n 
                     Target table: {target_table}\n 
                     Iceberg location: {iceberg_location}
                     """)

        response = glue.start_job_run(
            JobName=job_name,
            Arguments={
                "--SOURCE_PATH": source_path,
                "--TARGET_DB": target_db,
                "--TARGET_TABLE": target_table,
                "--ICEBERG_LOCATION": iceberg_location,
                "--GLUE_CATALOG": glue_catalog
            },
        )

        job_run_id = response["JobRunId"]
        
        logging.info(f"Glue job {job_name} started successfully")
        logging.info(f"Job Run ID: {job_run_id}")

        return job_run_id
    
    except ClientError as e:
        logging.error(f"AWS ClientError while starting Glue job {job_name}")

    except Exception as e:
        logging.error(f"Unexpected error while starting Glue job {job_name}")