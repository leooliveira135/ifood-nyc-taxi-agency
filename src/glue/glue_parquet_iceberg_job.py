import sys
import logging
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

def main(logger: logging):
    try:
        logger.info("Starting Parquet → Iceberg Glue job")

        sc = SparkContext()
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session
        logger.info("Spark session created")

        logger.info(f"Vectorized reader: {spark.conf.get('spark.sql.parquet.enableVectorizedReader')}")

        logger.info(f"sys.argv = {sys.argv}")
        args = getResolvedOptions(
            sys.argv,
            ["JOB_NAME", "SOURCE_DATABASE", "TABLE_NAME", "TARGET_DATABASE", "ICEBERG_LOCATION", "SOURCE_PATH"]
        )

        job = Job(glueContext)
        job.init(args["JOB_NAME"], args)

        source_database = args["SOURCE_DATABASE"]
        table_name = args["TABLE_NAME"]
        target_database = args["TARGET_DATABASE"]
        iceberg_location = args["ICEBERG_LOCATION"]
        source_path = args["SOURCE_PATH"]

        logger.info(
            "Job arguments | source_database=%s table_name=%s target_database=%s iceberg_location=%s source_path=%s",
            source_database, table_name, target_database, iceberg_location, source_path
        )
        logger.info(f"Reading Parquet table {source_path}")

        df = spark.read.parquet(source_path)
        df.printSchema()

        logger.info(
            "Parquet table loaded successfully | rows=%s columns=%s",
            df.count(),
            len(df.columns)
        )
        logger.info(
            "Writing Iceberg table glue_catalog.%s.%s",
            target_database, table_name
        )

        (
            df.writeTo(f"glue_catalog.{target_database}.{table_name}")
            .using("iceberg")
            .tableProperty("format-version", "2")
            .option("location", iceberg_location)
            .createOrReplace()
        )

        job.commit()

        logger.info("Iceberg table written successfully")

        logger.info("Parquet → Iceberg conversion completed")

    except Exception:
        logger.exception("Parquet → Iceberg job failed")
        raise

    
if __name__ == "__main__":

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
    logger = logging.getLogger("Parquet_to_iceberg")

    main(logger)

