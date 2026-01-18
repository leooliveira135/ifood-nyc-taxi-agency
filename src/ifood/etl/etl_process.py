import logging
from ifood.api.fetch_data import fetch_data_from_source
from ifood.aws.s3_bucket import check_file_exists_in_s3, download_file_to_s3, read_file_from_s3, write_data_into_s3
from ifood.vars import s3_raw_bucket, filename_list, filter_year, endpoint, s3_stg_bucket, s3_bucket, selected_columns, glue_database
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_date, lit

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
            s3_filename = file_url.split('/')[-1].strip()
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
    parquet_path = f"s3a://{s3_path}/{data.split('/')[-2]}"
    write_data_into_s3(parquet_path, df, partition_list=["source_date"])
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
    if ('VendorID' and 'passenger_count') and ('tpep_pickup_datetime' and 'tpep_dropoff_datetime') in valid_cols:
        df = df.withColumn("VendorID", col("VendorID").cast("integer").alias("VendorID"))
        df = df.withColumn("passenger_count", col("passenger_count").cast("integer").alias("passenger_count"))
        df = df.withColumn("tpep_pickup_datetime", col("tpep_pickup_datetime").cast("timestamp").alias("tpep_pickup_datetime"))
        df = df.withColumn("tpep_dropoff_datetime", col("tpep_dropoff_datetime").cast("timestamp").alias("tpep_dropoff_datetime"))
    table_name = df.select('data_source').distinct().collect()[0][0].split('/')[-1].split('.')[0]
    parquet_path = f"s3a://{s3_path}/{'_'.join(table_name.split('_')[0:2])}_parquet/{table_name.split('_')[-1].replace('-','_')}"
    write_data_into_s3(parquet_path, df)
    logging.info("Data loading completed.")

def run_etl_process(spark: SparkSession) -> None:
    """
        Run the Delta Lake pipeline for data processing.
        Args:
            spark (SparkSession): The Spark session object.
        Returns:
            None
    """
    logging.info("Starting Delta Lake pipeline")   
 
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
