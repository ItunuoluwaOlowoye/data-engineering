"""Constants for Lakeflow DQ."""
from typing import cast

from pyspark.sql import SparkSession

spark = cast(SparkSession, SparkSession.getActiveSession())

CATALOG = cast(str, spark.conf.get("catalog", "workspace"))
BRONZE_SCHEMA = cast(str, spark.conf.get("bronze_schema", "demo"))
SILVER_SCHEMA = cast(str, spark.conf.get("silver_schema", "demo"))
TABLE_NAME = "product"

VOLUME = cast(str, spark.conf.get("volume", "/Volumes/workspace/demo/files"))
