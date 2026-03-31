import pyspark.sql.functions as F
from pyspark import pipelines as dp
# Energy vs wealth 
@dp.table(
    name = "gold_energy_profile",
    description = "Energy vs wealth"
)
def gold_wealth_energy_yearly():
    # Energy generation: pct of renewables,
    # CO2 intensity: co2_intensity
    # WB: gdp per capita (to plot the wealth axis), income_level (to categorise countries)
    # Plot gdp agains co2 intensity and color by renewables %
    gdp_df = spark.read.table("energy_trans_dev.silver.silver_worldbank")
    gdp_df = gdp_df.filter(gdp_df.indicator == "gdp_per_capita")

    energy_df = spark.read.table("energy_trans_dev.silver.silver_ember_generation")
    renewable_df = energy_df.filter(energy_df.energy_category == "renewables")
    #Missing
    # co2_df 

    wealth_energy_df = renewable_df.alias("r").join(gdp_df.alias("g"), on=['country_code', 'year'], how="inner")\
        .select("r.country", "r.country_code", "g.year", "g.value","r.share_of_generation_pct")
    return wealth_energy_df

@dp.table(
    name="gold_access_gap"
)
def gold_access_gap(): 
    # Electricity access: elec_access_pct
    # Energy generation: pct of renewables,
    # See how involved in national grid renewables resource are. 
    energy_df = spark.read.table("energy_trans_dev.silver.silver_ember_generation")
    renewable_df = energy_df.filter(energy_df.energy_category == "renewables")

    wb_df = spark.read.table("energy_trans_dev.silver.silver_worldbank")
    el_access_df = wb_df.filter(wb_df.indicator == "elec_access_pct")

    access_energy_df = renewable_df.alias("r").join(el_access_df.alias("el"), on=['country_code', 'year'], how="inner")\
        .select("r.country", "r.country_code", "el.year", "el.value", "r.share_of_generation_pct")


@dp.table(
    name="gold_energy_generation_share"
)
def gold_energy_generation_share():
    # Energy generation: fuel_type, share_pct
    # Plot share of renewables by fuel type on choropleth map
    energy_df = spark.read.table("energy_trans_dev.silver.silver_ember_generation_monthly")\
        .withColumn("renewable_pct", F.when(F.col("energy_category") == "renewables", F.col("share_of_generation_pct")).otherwise(0))\
        .select("country", "country_code", "date", "energy_source", "energy_category", "share_of_generation_pct", "renewable_pct")

    return energy_df


@dp.table(
    name="gold_capacity_per_capita"
)
def gold_capacity_per_capita(): 
    # Energy generation: electricity_capacity
    # WB: population
    # Plot capacity per capita on choropleth map
    capacity_df = spark.read.table("energy_trans_dev.silver.silver_ember_capacity")\
        .withColumn("year", F.year(F.col("date")))\
        .groupBy("country_code", "year").agg(F.sum("capacity_gw"))

    wb_df = spark.read.table("energy_trans_dev.silver.silver_worldbank")
    pop_df = wb_df.filter(wb_df.indicator == "population")\


    capacity_pop_df = capacity_df.alias("c").join(pop_df.alias("p"), on=["country_code", "year"], how="inner")\
        .withColumn("capacity_per_capita", F.col("capacity_gw")/F.col("value"))\
        .select("c.country", "c.country_code", "c.year", "c.capacity_gw", "p.value", "capacity_per_capita")

    return capacity_pop_df


@dp.table(
    name="gold_coal_decline_rate"
)
def golcd_coal_decline_rate(): pass
    # coal_twh coal_share_pct coal_mom_delta coal_yoy_delta gdp_per_capita


@dp.table(
    name="gold_growth_transition"
)
def gold_growth_transition(): pass
    # gdp_growth_pct renewable_share_pct renewable_yoy_delta income_level


# --- Dim tables ---

@dp.table(
    name="dim_fuel_types"
)
def dim_fuel_types():
    # series fuel_group is_renewable is_fossil
    df = spark.read.table("energy_trans_dev.silver.silver_ember_generation")\
        .select("energy_source", "energy_category").dropDuplicates()
    return df

@dp.table(
    name="dim_country_codes"
)
def dim_country_codes():
    # entity_code entity_name iso2 income_level wb_region is_aggregate_entity
    df = spark.read.table("energy_trans_dev.silver.silver_ember_generation")\
        .select("country_name", "couuntry_codes").dropDuplicates()
    return df