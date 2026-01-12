import logging
from ifood.api.fetch_data import fetch_data_from_source
from ifood.aws.s3_bucket import check_file_exists_in_s3, download_file_to_s3, read_file_from_s3, write_data_into_s3
from ifood.aws.credentials import get_aws_credentials
from ifood.aws.glue_catalog import create_glue_database, create_glue_crawler, start_glue_crawler
from ifood.vars import (
    endpoint, filename_list, filter_year, s3_raw_bucket, aws_profile_name, s3_stg_bucket, s3_bucket, selected_columns, glue_database_stg, glue_database
)
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, col
from pyspark.sql.functions import current_date

def extract_data(endpoint: str, filename: str, date: str) -> str:
    """
        Extract data from the source and store it in S3.
        Args:
            endpoint (str): The URL to fetch data from.
            filename (str): The filename to process.
            date (str): The date string to filter the results.
        Returns:
            data (str): The S3 path of the last processed file.
    """
    logging.info("Starting data fetch process")
    logging.info(f"Processing {filename} for date: {date}")
    try:
        output = fetch_data_from_source(endpoint, filename, date)
        for file_url in output:
            bucket_key = ("_").join(file_url.split('/')[-1].split('_')[:2])
            s3_filename = file_url.split('/')[-1]
            if not check_file_exists_in_s3(s3_raw_bucket, bucket_key, s3_filename):
                logging.info(f"File {s3_filename} does not exist in S3. Downloading...")
                download_file_to_s3(file_url, s3_raw_bucket, bucket_key, s3_filename)
            data = read_file_from_s3(s3_raw_bucket, bucket_key, s3_filename)
            logging.info(f"File {s3_filename} in {s3_raw_bucket} is available at {data}")
            return data
    except ValueError as e:
        logging.error(e)

def transform_data(spark: SparkSession, data: str, s3_path: str):
    """
        Transform the extracted data and write it to S3 in Delta Lake format.
        Args:
            spark (SparkSession): The Spark session object.
            data (str): The data to transform.
            s3_path (str): The S3 path of the data to transform.
        Returns:
            df (DataFrame): The transformed Spark DataFrame.
    """
    logging.info(f"Reading data from {data}")
    df = spark.read.parquet(data)
    df = df.withColumn("ingestion_date", current_date())
    df = df.withColumn("data_source", lit(data))
    source_date = data.split('/')[-1].split('.')[-2].split('_')[-1]
    df = df.withColumn("source_date", lit(source_date))
    logging.info(f"DataFrame loaded from {s3_path} with {df.count()} records.")
    df.show(5, truncate=False)
    df.printSchema()
    delta_path = f"s3a://{s3_path}/{data.split('/')[-2]}"
    write_data_into_s3(delta_path, df, partition_list=["source_date"])
    return df

def load_data(df: DataFrame, s3_path: str, columns: list) -> None:
    """
        Load transformed data into S3 in Delta Lake format.
        Args:
            df (DataFrame): The transformed Spark DataFrame.
            s3_path (str): The S3 path where the data will be stored.
        Returns:
            None
    """
    logging.info(f"Loading data into Delta Lake format at {s3_path}")
    existing = set(df.columns)
    valid_cols = [c for c in columns if c in existing]
    if valid_cols:
        df = df.select(valid_cols)
    if 'VendorID' in valid_cols:
        df = df.withColumn("VendorID", col("VendorID").cast("integer").alias("VendorID"))
    table_name = df.select("data_source").distinct().collect()[0][0].split('/')[-1].split('.')[0]
    delta_path = f"s3a://{s3_path}/{'_'.join(table_name.split('_')[0:2])}/{table_name.split('_')[-1].replace('-','_')}"
    write_data_into_s3(delta_path, df)
    logging.info("Data loading completed.")

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


def main(spark: SparkSession):
    """
        Main function to orchestrate the ETL process.
        Args:
            spark (SparkSession): The Spark session object.
        Returns:
            None
    """

    logging.basicConfig(level=logging.INFO)
    logging.info("Starting data fetch process")

    get_months = [f"{i:02d}" for i in range(1, 6)]
    for filename in filename_list:
        for month in get_months:
            filter_date = f"{filter_year}-{month}"
            logging.info(f"Extracting data from {filename} source to S3...")
            data = extract_data(endpoint, filename, filter_date)
            logging.info("Data transformation completed.\n")

            logging.info(f"Writing transformed data to Delta Lake format in S3 bucket {s3_stg_bucket}...")
            df = transform_data(spark, data, s3_stg_bucket)
            logging.info("Data written to Delta Lake format in S3 successfully.\n")

            logging.info(f"Loading final data into S3 bucket {s3_bucket}...")
            load_data(df, s3_bucket, selected_columns)
            logging.info("Data loading to S3 completed successfully.")

    logging.info("Setting up AWS Glue catalog...")
    aws_credentials = get_aws_credentials(aws_profile_name)
    account_id = aws_credentials['account_id']
    aws_region = aws_credentials['region']
    glue_setup(aws_region, account_id)
    logging.info("AWS Glue catalog setup completed successfully.\n")

if __name__ == "__main__":

    spark = SparkSession.builder \
                        .appName("iFood Data Processing from NYC Taxi Agency") \
                        .config(
                            "spark.jars.packages",
                            "org.apache.hadoop:hadoop-aws:3.3.4,"
                            "io.delta:delta-spark_2.12:3.1.0,"
                            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
                        ) \
                        .config(
                             "spark.sql.catalog.spark_catalog",
                             "org.apache.spark.sql.delta.catalog.DeltaCatalog"
                        ) \
                        .config(
                            "spark.hadoop.fs.s3a.aws.credentials.provider",
                            "com.amazonaws.auth.profile.ProfileCredentialsProvider"
                        ) \
                        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                        .config("spark.hadoop.fs.s3a.profile", aws_profile_name) \
                        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
                        .config("spark.driver.memory", "6g") \
                        .config("spark.driver.maxResultSize", "2g") \
                        .config("spark.sql.shuffle.partitions", "8") \
                        .getOrCreate()
    
    main(spark)

    spark.stop()