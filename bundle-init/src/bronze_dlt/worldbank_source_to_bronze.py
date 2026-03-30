# Databricks notebook source
import dlt
from pyspark.sql.functions import col, current_timestamp

@dlt.table(
    name="bronze_worldbank_raw",
    comment="World Bank indicators ingested via Zerobus — 30 countries, 2015-2024"
)
def bronze_worldbank_raw():
    return (
        spark.read
        .table("energy_trans_dev.bronze.bronze_worldbank_zerobus")
        .withColumn("dlt_ingested_at", current_timestamp())
    )
