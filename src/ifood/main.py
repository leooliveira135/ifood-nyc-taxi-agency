import logging
from ifood.aws.credentials import get_aws_credentials
from ifood.etl.etl_process import run_etl_process
from ifood.etl.glue_setup import run_glue_catalog
from ifood.vars import aws_profile_name, iceberg_bucket
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

    run_etl_process(spark)

    aws_credentials = get_aws_credentials(aws_profile_name)
    run_glue_catalog(aws_credentials)

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("iFood Data Processing from NYC Taxi Agency")

        # --- Packages ---
        .config(
            "spark.jars.packages",
            "io.delta:delta-core_2.12:2.4.0,"
            "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.3,"
            "org.apache.iceberg:iceberg-aws:1.4.3"
        )

        # --- Spark SQL Extensions (MUST be single line) ---
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension,"
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )

        # --- Delta Catalog ---
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )

        # --- Iceberg Glue Catalog ---
        .config(
            "spark.sql.catalog.glue_catalog",
            "org.apache.iceberg.spark.SparkCatalog"
        )
        .config(
            "spark.sql.catalog.glue_catalog.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog"
        )
        .config(
            "spark.sql.catalog.glue_catalog.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO"
        )
        .config(
            "spark.sql.catalog.glue_catalog.warehouse",
            f"s3://{iceberg_bucket}"
        )

        # --- AWS credentials (local only) ---
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.profile.ProfileCredentialsProvider"
        )
        .config("spark.hadoop.fs.s3a.profile", aws_profile_name)

        # --- Performance / compatibility ---
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "4g")
        .config("spark.driver.maxResultSize", "2g")

        .getOrCreate()
    )

    main(spark)

    spark.stop()