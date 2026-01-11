import boto3
import requests
import logging
import time
from botocore.exceptions import ClientError
from ifood.vars import headers
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
        object_data.write \
            .format("delta") \
            .option("overwriteSchema", "true") \
            .mode("overwrite") \
            .save(path) \
            .partitionBy(partition_list) if partition_list else object_data.write \
            .format("delta") \
            .option("overwriteSchema", "true") \
            .mode("overwrite") \
            .save(path)
        logging.info(f"Data successfully written to Delta Lake at {path}")
    except Exception as e:
        logging.error(f"Failed to write data to Delta Lake at {path}: {e}")
        raise