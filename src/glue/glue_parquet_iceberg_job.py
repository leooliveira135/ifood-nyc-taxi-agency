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

        df = (
            spark.read
            .option("recursiveFileLookup", "true")
            .parquet(source_path)
        )

        if df.rdd.isEmpty():
            logging.error(f"No Parquet data found at {source_path}")

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

        partition_cols = []
        if "source_date" in df.columns:
            partition_cols.append("source_date")

        logger.info(f"Detected partition columns: {partition_cols}")
        
        table = f"{target_database}.{table_name}"

        table_exists = (
            spark.sql(f"SHOW TABLES IN {target_database}")
            .filter(f"tableName = '{table}'")
            .count() > 0
        )

        if not table_exists:
            logger.info(f"Creating Iceberg table {table}")

            (
                df.writeTo(table)
                .tableProperty("format-version", "2")
                .option("location", iceberg_location)
                .create()
            )

            logger.info("Adding Iceberg partition spec: source_date")

            spark.sql(f"""
                ALTER TABLE {table}
                ADD PARTITION FIELD source_date
            """)

        else:
            logger.info(f"Appending to Iceberg table {table}")
            df.writeTo(table).append()

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

