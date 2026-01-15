import sys
import logging
from awsglue.utils import getResolvedOptions
from ifood.vars import s3_stg_bucket
from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("delta_to_iceberg")

def main():
    try:
        logger.info("Starting Delta → Iceberg Glue job")
        args = getResolvedOptions(
            sys.argv,
            ["SOURCE_DATABASE", "TABLE_NAME", "TARGET_DATABASE", "ICEBERG_LOCATION"]
        )

        source_database = args["SOURCE_DATABASE"]
        table_name = args["TABLE_NAME"]
        target_database = args["TARGET_DATABASE"]
        iceberg_location = args["ICEBERG_LOCATION"]

        logger.info(
            "Job arguments | source_database=%s table_name=%s target_database=%s iceberg_location=%s",
            source_database, table_name, target_database, iceberg_location
        )
        source_path = f"s3://{s3_stg_bucket}/{table_name}"

        spark = SparkSession.builder.getOrCreate()
        logger.info("Spark session created")
        logger.info(f"Reading Delta table {source_path}")

        df = spark.read.format("delta").load(source_path)

        logger.info(
            "Delta table loaded successfully | rows=%s columns=%s",
            df.count(),
            len(df.columns)
        )
        logger.info(
            "Writing Iceberg table glue_catalog.%s.%s",
            target_database, table_name
        )

        (
            df.writeTo(f"{target_database}.{table_name}")
            .using("iceberg")
            .tableProperty("format-version", "2")
            .option("location", iceberg_location)
            .createOrReplace()
        )

        logger.info("Iceberg table written successfully")

        logger.info("Delta → Iceberg conversion completed")

    except Exception as e:
        logger.exception("Delta → Iceberg job failed")
        raise RuntimeError("Glue job failed") from e
    
if __name__ == "__main__":
    main()

