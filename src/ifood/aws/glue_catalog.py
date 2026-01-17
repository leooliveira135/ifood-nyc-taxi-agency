import boto3
import logging
import time
from botocore.exceptions import ClientError
from ifood.vars import aws_glue_role
from pathlib import Path

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

def start_glue_crawler(crawler_name: str, aws_region: str):
    """
        Start a Glue crawler to catalog data in S3.
        Args:
            crawler_name (str): The name of the Glue crawler to start.
            aws_region (str): The AWS region where the crawler will be created.
        Returns:
            None
    """
    glue = boto3.client('glue', region_name=aws_region)
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

def list_glue_db_tables(database_name: str, aws_region: str) -> list:
    """
        List all tables in a Glue database.
        Args:
            database_name (str): The name of the Glue database.
            aws_region (str): The AWS region where the crawler will be created.
        Returns:
            list: A list of table names in the database.
    """
    glue = boto3.client('glue', region_name=aws_region)
    db_list = glue.get_tables(DatabaseName=database_name)
    table_list = [table['Name'] for table in db_list['TableList']]
    logging.info(f"Tables in database {database_name}: {table_list}")
    return table_list

def create_glue_job(job_name:str, account_id: str, aws_region: str, glue_job_path: str, extra_py_files: str, glue_version: str="4.0", worker_type: str="G.1X", num_workers: int=5, timeout: int=60):
    """
        Create a Glue job for ETL processing.
        Args:
            job_name (str): The name of the Glue job to create.
            account_id (str): The AWS account ID.
            aws_region (str): The AWS region where the crawler will be created.
            glue_job_path (str): The S3 path to the Glue job script.
            extra_py_files (str | None): Optional S3 path to a .zip or .egg with Python dependencies. Example: s3://my-bucket/libs/my_libs.zip
            glue_version (str): The Glue version to use. Defaults to "4.0".
            worker_type (str): The type of worker to use. Defaults to "G.1X".
            num_workers (int): The number of workers to allocate. Defaults to 5.
            timeout (int): The job timeout in minutes. Defaults to 60.
        Returns:
            None
    """

    glue = boto3.client('glue', region_name=aws_region)
    role_arn = f"arn:aws:iam::{account_id}:role/{aws_glue_role}"

    if isinstance(glue_job_path, Path):
        glue_job_path = glue_job_path.as_posix()
        
    if not glue_job_path:
        logging.error(f"{glue_job_path} is None — upload failed")
        
    if not glue_job_path.startswith("s3://"):
        logging.error(f"Invalid ScriptLocation (must be s3://): {glue_job_path}")

    try:
        glue.get_job(JobName=job_name)
        logging.error(f"Glue job {job_name} already exists — skipping creation")
        return

    except glue.exceptions.EntityNotFoundException:
        logging.info(f"Glue job '{job_name}' not found — creating it")

    try:

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
                "--enable-continuous-cloudwatch-log": "true",
                "--extra-py-files": extra_py_files,
            },
            GlueVersion=glue_version,
            WorkerType=worker_type,
            NumberOfWorkers=num_workers,
            Timeout=timeout
        )

        logging.info(f"Glue job {job_name} created successfully")

    except ClientError as e:
        logging.error(f"""AWS ClientError while creating Glue job {job_name}\n
                      {e.response['Error']['Code']} - {e.response['Error']['Message']}""")

    except Exception as e:
        logging.error(f"Unexpected error while creating Glue job {job_name}")

def run_glue_job(job_name:str, table_name: str, source_database: str, target_database: str, iceberg_location:str, aws_region: str, source_path: str) -> str:
    """
        Run a Glue job for ETL processing.
        Args:
            job_name (str): The name of the Glue job to run.
            table_name (str): The name of the table sent to Glue job.
            source_database (str): The source Glue database name.
            target_database (str): The target Glue database name.
            iceberg_location (str): The S3 location for Iceberg table data.
            aws_region (str): The AWS region where the crawler will be created.
            source_path (str): The S3 path where the data is stored.
        Returns:
            str: The job run ID.
    """

    glue = boto3.client('glue', region_name=aws_region)

    try:
        logging.info(f"Starting Glue job: {job_name}")
        logging.info(f"""Job arguments\n 
                     Source database: {source_database}\n 
                     Table name: {table_name}\n 
                     Target database: {target_database}\n 
                     Iceberg location: {iceberg_location}\n 
                     Source Path: {source_path}
                     """)

        response = glue.start_job_run(
            JobName=job_name,
            Arguments={
                "--SOURCE_DATABASE": source_database,
                "--TABLE_NAME": table_name,
                "--TARGET_DATABASE": target_database,
                "--ICEBERG_LOCATION": iceberg_location,
                "--SOURCE_PATH": source_path
            },
        )

        job_run_id = response["JobRunId"]

        logging.info(f"Glue job {job_name} started successfully")
        logging.info(f"Job Run ID: {job_run_id}")
        logging.info(f"JobRun Arguments: {job_run['JobRun'].get('Arguments')}")  

        return job_run_id
    
    except ClientError as e:
        logging.error(f"AWS ClientError while starting Glue job {job_name}")
        raise

    except Exception as e:
        logging.error(f"Unexpected error while starting Glue job {job_name}")
        raise

def wait_for_glue_job_completion(glue_client, job_name: str, job_run_id: str, poll_seconds: int = 30, timeout_seconds: int | None = None,) -> Dict[str, Any]:
    """
        Block execution until an AWS Glue job run reaches a terminal state.

        This function continuously polls the Glue job status until the job
        finishes or an optional timeout is reached.

        Args:
            glue_client: Boto3 Glue client instance.
            job_name (str): Name of the Glue job.
            job_run_id (str): Identifier of the Glue job run.
            poll_seconds (int, optional): Interval (in seconds) between status checks. Defaults to 30 seconds.
            timeout_seconds (int, optional): Maximum time to wait for job completion. If None, waits indefinitely.

        Returns:
            Dict[str, Any]: Full JobRun metadata returned by AWS Glue.

        Raises:
            TimeoutError: If the job does not complete within timeout_seconds.
    """

    start_time = time.time()

    while True:
        response = glue_client.get_job_run(
            JobName=job_name,
            RunId=job_run_id,
            PredecessorsIncluded=False,
        )

        job_run = response["JobRun"]
        state = job_run["JobRunState"]

        logging.info(f"Glue job {job_name} (run_id={job_run_id}) status: {state}")

        if state in ("SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT"):
            return job_run

        if timeout_seconds is not None:
            elapsed = time.time() - start_time
            if elapsed > timeout_seconds:
                logging.error(f"Glue job {job_name} ({job_run_id}) did not complete within {timeout_seconds} seconds")

        time.sleep(poll_seconds)
