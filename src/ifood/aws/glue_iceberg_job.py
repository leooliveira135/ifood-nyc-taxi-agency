import sys
import logging
from awsglue.utils import getResolvedOptions
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
            ["SOURCE_PATH", "TARGET_DB", "TARGET_TABLE", "ICEBERG_LOCATION", "GLUE_CATALOG"]
        )

        source_path = args["SOURCE_PATH"]
        target_db = args["TARGET_DB"]
        target_table = args["TARGET_TABLE"]
        iceberg_location = args["ICEBERG_LOCATION"]
        glue_catalog = args["GLUE_CATALOG"]

        logger.info(
            "Job arguments | source_path=%s target_db=%s target_table=%s iceberg_location=%s glue_catalog=%s",
            source_path, target_db, target_table, iceberg_location, glue_catalog
        )

        spark = SparkSession.builder.getOrCreate()
        logger.info("Spark session created")
        logger.info(f"Reading Delta table from {source_path}")

        df = spark.read.format("delta").load(source_path)

        logger.info(
            "Delta table loaded successfully | rows=%s columns=%s",
            df.count(),
            len(df.columns)
        )
        logger.info(
            "Writing Iceberg table glue_catalog.%s.%s",
            target_db, target_table
        )

        (
            df.writeTo(f"{glue_catalog}.{target_db}.{target_table}")
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

