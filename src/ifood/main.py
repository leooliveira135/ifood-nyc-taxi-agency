import logging
from ifood.aws.credentials import get_aws_credentials
from ifood.etl.etl_process import run_etl_process
from ifood.etl.glue_setup import run_glue_catalog
from ifood.vars import aws_profile_name
from pyspark.sql import SparkSession

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

    # run_etl_process(spark)

    aws_credentials = get_aws_credentials(aws_profile_name)
    run_glue_catalog(aws_credentials)

if __name__ == "__main__":

    spark = SparkSession.builder \
                        .appName("iFood Data Processing from NYC Taxi Agency") \
                        .config(
                            "spark.jars.packages",
                            "org.apache.hadoop:hadoop-aws:3.3.4,"
                            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
                        ) \
                        .config(
                            "spark.hadoop.fs.s3a.aws.credentials.provider",
                            "com.amazonaws.auth.profile.ProfileCredentialsProvider"
                        ) \
                        .config(
                            "spark.sql.catalog.glue_catalog",
                            "org.apache.iceberg.spark.SparkCatalog"
                        ) \
                        .config(
                            "spark.sql.catalog.glue_catalog.catalog-impl",
                            "org.apache.iceberg.aws.glue.GlueCatalog"
                        ) \
                        .config(
                            "spark.sql.catalog.glue_catalog.io-impl",
                            "org.apache.iceberg.aws.s3.S3FileIO"
                        ) \
                        .config(
                            "spark.sql.catalog.glue_catalog.warehouse",
                            "s3://your-warehouse-bucket/iceberg/"
                        ) \
                        .config("spark.hadoop.fs.s3a.profile", aws_profile_name) \
                        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
                        .config("spark.driver.memory", "6g") \
                        .config("spark.driver.maxResultSize", "2g") \
                        .config("spark.sql.shuffle.partitions", "8") \
                        .getOrCreate()

    main(spark)

    spark.stop()