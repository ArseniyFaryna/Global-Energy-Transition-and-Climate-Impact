# COMMAND ----------

%pip install isocodes

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import col, to_date
from pyspark.sql.functions import when
from isocodes import countries
# Country helper df

country_list = [
    {"alpha_2": c["alpha_2"], "alpha_3": c["alpha_3"]}
    for c in countries.items
    if "alpha_2" in c and "alpha_3" in c
]
lookup_df = spark.createDataFrame(country_list)

@dp.materialized_view(
    name="silver_ember_country_yearly",
    comment="Ember country-level yearly metrics: CO2 intensity, electricity demand, power sector emissions",
)
@dp.expect("valid_year", "year IS NOT NULL AND year >= 2015")
@dp.expect("valid_country", "country_name IS NOT NULL")
@dp.expect("valid_country_code", "country_code IS NOT NULL")
def silver_ember_country_yearly():
    carbon = (
        spark.read.table("energy_trans_dev.bronze.bronze_carbon_intensity_yearly")
        .filter(col("is_aggregate_entity") == False)
        .select(
            col("entity").alias("country_name"),
            col("entity_code").alias("country_code"),
            col("date").cast("int").alias("year"),
            col("emissions_intensity_gco2_per_kwh").alias("co2_intensity_gco2_per_kwh"),
            col("_ingested_at"),
        )
    )

    demand = (
        spark.read.table("energy_trans_dev.bronze.bronze_electricity_demand_yearly")
        .filter(col("is_aggregate_entity") == False)
        .select(
            col("entity").alias("country_name"),
            col("date").cast("int").alias("year"),
            col("demand_twh"),
            col("demand_mwh_per_capita"),
        )
    )

    emissions = (
        spark.read.table("energy_trans_dev.bronze.bronze_power_sector_emissions_yearly")
        .filter(col("is_aggregate_entity") == False)
        .filter(col("series") == "Total generation")
        .filter(col("is_aggregate_series") == True)
        .select(
            col("entity").alias("country_name"),
            col("date").cast("int").alias("year"),
            col("emissions_mtco2").alias("total_emissions_mtco2"),
        )
    )

    return (
        carbon
        .join(demand, on=["country_name", "year"], how="left")
        .join(emissions, on=["country_name", "year"], how="left")
        .dropDuplicates(["country_name", "year"])
    )

@dp.materialized_view(
    name="silver_ember_generation",
    comment="Ember electricity generation by energy source per country and per year",
)
@dp.expect("valid_year", "year IS NOT NULL AND year >= 2015")
@dp.expect("valid_country", "country_name IS NOT NULL")
@dp.expect("valid_generation", "generation_twh IS NOT NULL")
def silver_ember_generation():
    return (
        spark.read.table("energy_trans_dev.bronze.bronze_electricity_generation_yearly")
        .filter(col("is_aggregate_entity") == False)
        .filter(col("is_aggregate_series") == False)
        .withColumn("year", col("date").cast("int"))
        .withColumn(
            "energy_category",
            when(col("series").isin("Solar", "Wind", "Hydro", "Bioenergy", "Other renewables"), "renewables")
            .when(col("series").isin("Coal", "Gas", "Other fossil"), "fossil")
            .when(col("series") == "Nuclear", "nuclear")
            .otherwise("other")
        )
        .select(
            col("entity").alias("country_name"),
            col("entity_code").alias("country_code"),
            "year",
            col("series").alias("energy_source"),
            "energy_category",
            "generation_twh",
            "share_of_generation_pct",
            col("_ingested_at"),
        )
        .dropDuplicates(["country_name", "year", "energy_source"])
    )


@dp.materialized_view(
    name="silver_ember_generation_monthly",
    comment="Ember electricity generation by energy source per country (monthly) — for coal decline analysis",
)
@dp.expect("valid_date", "date IS NOT NULL")
@dp.expect("valid_country", "country_name IS NOT NULL")
@dp.expect("valid_generation", "generation_twh IS NOT NULL")
def silver_ember_generation_monthly():
    return (
        spark.read.table("energy_trans_dev.bronze.bronze_electricity_generation_monthly")
        .filter(col("is_aggregate_entity") == False)
        .filter(col("is_aggregate_series") == False)
        .withColumn(
            "energy_category",
            when(col("series").isin("Solar", "Wind", "Hydro", "Bioenergy", "Other renewables"), "renewables")
            .when(col("series").isin("Coal", "Gas", "Other fossil"), "fossil")
            .when(col("series") == "Nuclear", "nuclear")
            .otherwise("other")
        )
        .select(
            col("entity").alias("country_name"),
            col("entity_code").alias("country_code"),
            to_date(col("date")).alias("date"),
            col("series").alias("energy_source"),
            "energy_category",
            "generation_twh",
            "share_of_generation_pct",
            col("_ingested_at"),
        )
        .dropDuplicates(["country_name", "date", "energy_source"])
    )


@dp.materialized_view(
    name="silver_ember_capacity",
    comment="Ember installed capacity by energy source per country (monthly)",
)
@dp.expect("valid_date", "date IS NOT NULL")
@dp.expect("valid_country", "country_name IS NOT NULL")
@dp.expect("valid_capacity", "capacity_gw IS NOT NULL")
def silver_ember_capacity():
    return (
        spark.read.table("energy_trans_dev.bronze.bronze_installed_capacity_monthly")
        .filter(col("is_aggregate_entity") == False)
        .filter(col("is_aggregate_series") == False)
        .select(
            col("entity").alias("country_name"),
            col("entity_code").alias("country_code"),
            to_date(col("date")).alias("date"),
            col("series").alias("energy_source"),
            col("capacity_gw"),
            col("capacity_w_per_capita"),
            col("_ingested_at"),
        )
        .dropDuplicates(["country_name", "date", "energy_source"])
    )


@dp.materialized_view(
    name="silver_worldbank",
    comment="World Bank indicators cleaned: GDP, population, electricity access, renewables share",
)
@dp.expect("valid_year", "year IS NOT NULL AND year >= 2015")
@dp.expect("valid_country", "country_name IS NOT NULL")
@dp.expect("valid_value", "value IS NOT NULL")
def silver_worldbank():
    wb = spark.read.table("energy_trans_dev.bronze.bronze_worldbank_raw")
    return (
        wb.join(lookup_df, wb.country_code == lookup_df.alpha_2, "left")
        .withColumn("country_code", col("alpha_3"))
        .drop("alpha_2", "alpha_3")
        .select(
            col("country").alias("country_name"),
            col("country_code"),
            col("indicator"),
            col("indicator_code"),
            col("year").cast("int").alias("year"),
            col("value").cast("double").alias("value"),
            col("ingested_at"),
        )
        .filter(col("value").isNotNull())
        .filter(col("year").cast("int") >= 2015)
        .dropDuplicates(["country_name", "year", "indicator"])
    )