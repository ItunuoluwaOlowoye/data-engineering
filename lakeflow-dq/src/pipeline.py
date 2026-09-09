"""Pipeline definitions."""
from typing import cast

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from constants import BRONZE_SCHEMA, CATALOG, SILVER_SCHEMA, TABLE_NAME, VOLUME
from helpers import table_fqn
from schema import (
    BRONZE_OUTPUT_SCHEMA,
    QUARANTINE_OUTPUT_SCHEMA,
    SILVER_OUTPUT_SCHEMA,
)

spark = cast(SparkSession, SparkSession.getActiveSession())

BRONZE_TABLE = table_fqn(catalog=CATALOG, schema=BRONZE_SCHEMA, table_name=TABLE_NAME)
SILVER_TABLE = table_fqn(catalog=CATALOG, schema=SILVER_SCHEMA, table_name=TABLE_NAME)
QUARANTINE_TABLE = table_fqn(catalog=CATALOG, schema=SILVER_SCHEMA, table_name=f"{TABLE_NAME}_quarantine")

@dp.table(
    name=BRONZE_TABLE,
    comment="Bronze table: raw product data",
    table_properties={"quality": "bronze"},
)
def product_bronze():
    """Read raw product data into the bronze table."""
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", True)
        .load(f"{VOLUME}/products")
        .select(
            "*",
            F.current_timestamp().alias("_loaded_datetime"),
            F.col("_metadata.file_path").alias("_source_file"),
        )
    )

    return df.select(BRONZE_OUTPUT_SCHEMA.fieldNames())

@dp.table(
    name=SILVER_TABLE,
    comment="Silver table: cleaned product data",
    table_properties={"quality": "silver"},
)
# Expectations
@dp.expect("sku_format", "sku_code IS NULL OR length(sku_code) = 8")
@dp.expect_or_fail("primary_key", "product_id IS NOT NULL")
def product_silver():
    """Read cleaned product data into the silver table."""
    return dp.read_stream(BRONZE_TABLE).select(SILVER_OUTPUT_SCHEMA.fieldNames())

@dp.table(
    name=QUARANTINE_TABLE,
    comment="Quarantine table for invalid product data",
    table_properties={"quality": "quarantine"},
)
#Expectations
@dp.expect_or_drop("sku_format", "sku_code IS NOT NULL AND length(sku_code) <> 8")
@dp.expect_or_drop("primary_key", "product_id IS NULL")
def product_quarantine():
    """Read invalid product data into the quarantine table."""
    return dp.read_stream(BRONZE_TABLE).select(QUARANTINE_OUTPUT_SCHEMA.fieldNames())
