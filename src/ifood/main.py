import logging
from ifood.api.fetch_data import fetch_data_from_source
from ifood.aws.s3_bucket import check_file_exists_in_s3, download_file_to_s3, read_file_from_s3, write_data_into_s3
from ifood.aws.credentials import get_aws_credentials
from ifood.vars import endpoint, filename_list, filter_year, s3_raw_bucket, aws_profile_name, s3_stg_bucket
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
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
        Transform data stored in S3 using Spark.
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

def main(spark: SparkSession):
    """
        Main function to fetch data from source and store it in S3.
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
                        .getOrCreate()
    
    main(spark)

    spark.stop()