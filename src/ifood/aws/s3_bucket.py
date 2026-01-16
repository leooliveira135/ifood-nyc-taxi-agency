import boto3
import requests
import logging
import time
from botocore.exceptions import ClientError
from ifood.vars import headers
from pathlib import Path
from pyspark.sql import DataFrame
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def check_file_exists_in_s3(bucket_name: str, s3_key: str, filename: str) -> bool:
    """
        Check if a file exists in an S3 bucket.
        Args:
            bucket_name (str): The name of the S3 bucket.
            s3_key (str): The S3 key (path) where the file is stored.
            filename (str): The name of the file to check.
        Returns:
            bool: True if the file exists, False otherwise.
    """
    s3 = boto3.client('s3')
    complete_path = s3_key + f"/{filename}"
    try:
        s3.head_object(Bucket=bucket_name, Key=complete_path)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            logging.error(f"AWS ClientError: {e}")
            raise

def download_file_to_s3(url: str, bucket_name: str, s3_key: str, filename: str) -> None:
    """
        Download a file from a URL and upload it to an S3 bucket.
        Args:
            url (str): The URL of the file to download.
            bucket_name (str): The name of the S3 bucket.
            s3_key (str): The S3 key (path) where the file will be stored.
            filename (str): The name of the file being uploaded.
        Returns:
            None
    """
    s3 = boto3.client('s3')
    session = requests.Session()
    session.headers.update(headers)
    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[403, 429, 500, 502, 503, 504],
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    response = session.get(url.strip(), stream=True, headers=headers)
    time.sleep(10)

    if response.status_code != 200:
        logging.error(f"Failed to download file from {url}, status code: {response.status_code}")
        logging.error(f"Response content: {response.text}")
        raise ValueError(f"Failed to download file from {url}")
    else:
        complete_path = s3_key + f"/{filename}"
        try:
            logging.info(f"Downloading {url} to s3://{bucket_name}/{s3_key}")
            response.raise_for_status()
            s3.upload_fileobj(response.raw, bucket_name, complete_path)
            logging.info(f"File {filename} successfully uploaded to s3://{bucket_name}/{s3_key}")
        except ClientError as ce:
            logging.error(f"AWS ClientError: {ce}")
            raise
        except Exception as e:
            logging.error(f"Failed to upload {url} to S3: {e}")
            raise

def read_file_from_s3(bucket_name: str, s3_key: str, filename: str) -> bytes:
    """
        Read a file from an S3 bucket.
        Args:
            bucket_name (str): The name of the S3 bucket.
            s3_key (str): The S3 key (path) where the file is stored.
            filename (str): The name of the file to read.
        Returns:
            bytes: The content of the file.
    """
    s3 = boto3.client('s3')
    complete_path = s3_key + f"/{filename}"
    try:
        logging.info(f"Reading s3://{bucket_name}/{s3_key}/{filename}")
        paginator = s3.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=complete_path)
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    data = f"s3a://{bucket_name}/{obj['Key']}"
                    logging.info(f"File {filename} found in S3.")
                    return data
    except ClientError as ce:
        logging.error(f"AWS ClientError: {ce}")
        raise
    except Exception as e:
        logging.error(f"Failed to read {filename} from S3: {e}")
        raise

def write_data_into_s3(path: str, object_data: DataFrame, partition_list: list=None) -> None:
    """
        Write data into S3 in Delta Lake format.
        Args:
            path (str): The S3 path where the data will be written.
            object_data (DataFrame): The Spark DataFrame to write.
            partition_list (list, optional): List of columns to partition the data by.
        Returns:
            None
    """
    logging.info(f"Writing data to Delta Lake at {path} with partitions {partition_list}")
    try:
        if partition_list:
            object_data.write \
                .format("delta") \
                .option("overwriteSchema", "true") \
                .mode("overwrite") \
                .partitionBy(*partition_list) \
                .save(path)
        else:
            object_data.write \
                .format("delta") \
                .option("overwriteSchema", "true") \
                .mode("append") \
                .save(path)
        logging.info(f"Data successfully written to Delta Lake at {path}")
    except Exception as e:
        logging.error(f"Failed to write data to Delta Lake at {path}: {e}")
        raise

def upload_file_s3_bucket(bucket_name: str, bucket_key:str, local_path: Path, aws_region: str):
    """
        Upload a file to an S3 bucket.
        Args:
            bucket_name (str): The name of the S3 bucket.
            bucket_key (str): The S3 prefix including filename (e.g., scripts/glue_iceberg_job.py).
            local_path (Path): The local file path to upload.
            aws_region (str): The AWS region where the crawler will be created.
        Returns:
            script_path (str): The S3 key of the uploaded file.
    """
    s3 = boto3.client('s3', region_name=aws_region)
    local_path = Path(local_path)

    if not local_path.is_file():
        logging.error(f"Glue script not found in {local_path}")
    
    logging.info(f"Uploading {local_path} to s3://{bucket_name}/{bucket_key}/{str(local_path).split('/')[-1]}")

    try:
        s3.upload_file(
            Filename=local_path.as_posix(),
            Bucket=bucket_name,
            Key=f"{bucket_key}/{str(local_path).split('/')[-1]}"
        )

        s3.head_object(Bucket=bucket_name, Key=bucket_key)

        script_path = f"s3://{bucket_name}/{bucket_key}"
        logging.info(f"Upload completed successfully: {script_path}")
        return script_path

    except ClientError as e:
        logging.error(f"""AWS error during upload: {e}\n
                      {e.response['Error']['Code']} - {e.response['Error']['Message']}""")

    except Exception as e:
        logging.error(f"Unexpected error during upload: {e}")