import boto3
import logging
import time
from ifood.aws.credentials import get_aws_credentials
from ifood.vars import aws_profile_name

def create_athena_client(region_name: str, access_key: str, secret_key: str) -> boto3.client:
    """
        Create an Athena client using boto3.
        Args:
            region_name (str): The AWS region name.
            access_key (str): The AWS access key ID.
            secret_key (str): The AWS secret access key.
        Returns:
            boto3.client: The Athena client.
    """
    try:
        athena_client = boto3.client('athena', region_name=region_name, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        logging.info("Athena client created successfully.")
        return athena_client
    except Exception as e:
        logging.error(f"Failed to create Athena client: {e}")
        raise

def athena_start_query_execution(athena_client: boto3.client, query: str, output_location: str) -> str:
    """
        Start an Athena query execution.
        Args:
            athena_client (boto3.client): The Athena client.
            query (str): The SQL query to execute.
            output_location (str): The S3 location for query results.
        Returns:
            str: The query execution ID.
    """
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            ResultConfiguration={'OutputLocation': output_location}
        )
        query_execution_id = response['QueryExecutionId']
        logging.info(f"Athena query started with execution ID: {query_execution_id}")
        return query_execution_id
    except Exception as e:
        logging.error(f"Failed to start Athena query execution: {e}")
        raise

def athena_get_query_results(athena_client: boto3.client, query_execution_id: str) -> list:
    """
        Get the results of an Athena query execution.
        Args:
            athena_client (boto3.client): The Athena client.
            query_execution_id (str): The query execution ID.
        Returns:
            list: The query results.
    """
    try:
        results = []
        paginator = athena_client.get_paginator('get_query_results')
        for page in paginator.paginate(QueryExecutionId=query_execution_id):
            results.extend(page['ResultSet']['Rows'])
        logging.info(f"Retrieved {len(results)} rows from Athena query results.")
        return results
    except Exception as e:
        logging.error(f"Failed to get Athena query results: {e}")
        raise

def athena_wait_for_query_completion(athena_client: boto3.client, query_execution_id: str) -> None:
    """
        Wait for an Athena query to complete.
        Args:
            athena_client (boto3.client): The Athena client.
            query_execution_id (str): The query execution ID.
        Returns:
            None
    """
    while True:
        response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        state = response['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            logging.info(f"Athena query execution {query_execution_id} completed with state: {state}")
            if state != 'SUCCEEDED':
                raise Exception(f"Athena query execution failed with state: {state}")
            break
        else:
            logging.info(f"Athena query execution {query_execution_id} is still running. Waiting...")
            time.sleep(5)

def transform_athena_results_to_dataframe(results: list) -> list:
    """
        Transform Athena query results into a list of dictionaries.
        Args:
            results (list): The Athena query results.
        Returns:
            list: A list of dictionaries representing the query results.
    """
    if not results or len(results) < 2:
        logging.warning("No results found or insufficient data.")
        return []

    headers = [col['VarCharValue'] for col in results[0]['Data']]
    data = []
    for row in results[1:]:
        row_data = {}
        for idx, col in enumerate(row['Data']):
            row_data[headers[idx]] = col.get('VarCharValue', None)
        data.append(row_data)

    logging.info(f"Transformed Athena results into {len(data)} records.")
    return data

def execute_athena_query(region_name: str, access_key: str, secret_key: str, query: str, output_location: str) -> list:
    """
        Execute an Athena query and return the results as a list of dictionaries.
        Args:
            region_name (str): The AWS region name.
            access_key (str): The AWS access key ID.
            secret_key (str): The AWS secret access key.
            query (str): The SQL query to execute.
            output_location (str): The S3 location for query results.
        Returns:
            list: A list of dictionaries representing the query results.
    """
    aws_credentials = get_aws_credentials(aws_profile_name)
    region_name = aws_credentials['region']
    access_key = aws_credentials['access_key']
    secret_key = aws_credentials['secret_key']
    athena_client = create_athena_client(region_name, access_key, secret_key)
    query_execution_id = athena_start_query_execution(athena_client, query, output_location)
    athena_wait_for_query_completion(athena_client, query_execution_id)
    results = athena_get_query_results(athena_client, query_execution_id)
    transformed_results = transform_athena_results_to_dataframe(results)
    return transformed_results