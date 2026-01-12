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
            'account_id': account_id,
            'region': region,
            'access_key': credentials.access_key,
            'secret_key': credentials.secret_key
        }
    except Exception as e:
        logging.error(f"Error retrieving AWS credentials for profile {profile_name}: {e}")
        raise
