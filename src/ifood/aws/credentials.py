import boto3
import logging

def get_aws_credentials(profile_name: str):
    """
        Retrieve AWS credentials using a specified profile name.
        Args:
            profile_name (str): The AWS profile name.
        Returns:
            dict: A dictionary containing AWS access key, secret key, and session token.
    """
    try:
        session = boto3.Session(profile_name=profile_name)
        credentials = session.get_credentials().get_frozen_credentials()
        account_id = session.client('sts').get_caller_identity().get('Account')
        region = session.region_name
        logging.info(f"Retrieved AWS credentials for profile {profile_name} in account {account_id}")
        return {
            'aws_access_key_id': credentials.access_key,
            'aws_secret_access_key': credentials.secret_key,
            'region': region
        }
    except Exception as e:
        logging.error(f"Error retrieving AWS credentials for profile {profile_name}: {e}")
        raise

def create_deltalake_storage_options(access_key: str, secret_key: str, region_name: str) -> dict:
    """
        Create storage options for Delta Lake S3 access.
        Args:
            access_key (str): AWS access key ID.
            secret_key (str): AWS secret access key.
            region_name (str): AWS region name.
        Returns:
            dict: Storage options for Delta Lake S3 access.
    """
    logging.info("Creating Delta Lake storage options for S3 access")
    storage_options = {
        "AWS_REGION": region_name,
        "AWS_ACCESS_KEY_ID": access_key,
        "AWS_SECRET_ACCESS_KEY": secret_key,
        'AWS_S3_ALLOW_UNSAFE_RENAME': 'true',
    }
    logging.info("Delta Lake storage options created successfully")
    return storage_options