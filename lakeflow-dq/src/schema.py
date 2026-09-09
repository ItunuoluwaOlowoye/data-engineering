"""Schemas for the Lakeflow DQ pipeline."""
import copy

from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

BRONZE_OUTPUT_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), True),
        StructField("sku_code", StringType(), True),
        StructField("category", StringType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("last_updated_date", DateType(), True),
        StructField("_loaded_datetime", TimestampType(), False),
        StructField("_source_file", StringType(), False),
    ]
)

QUARANTINE_OUTPUT_SCHEMA = copy.copy(BRONZE_OUTPUT_SCHEMA)

SILVER_OUTPUT_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), True),
        StructField("sku_code", StringType(), True),
        StructField("category", StringType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("last_updated_date", DateType(), True),
    ]
)