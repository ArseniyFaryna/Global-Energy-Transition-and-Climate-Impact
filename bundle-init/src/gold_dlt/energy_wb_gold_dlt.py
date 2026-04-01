import pyspark.sql.functions as F
from pyspark import pipelines as dp
from pyspark.sql.window import Window

# Energy vs wealth 
@dp.table(
    name = "gold_energy_profile",
)
def gold_wealth_energy_yearly():
    # Energy generation: pct of renewables,
    # CO2 intensity: co2_intensity
    # WB: gdp per capita (to plot the wealth axis), income_level (to categorise countries)
    # Plot gdp agains co2 intensity and color by renewables %
    gdp_df = spark.read.table("energy_trans_dev.silver.silver_worldbank")
    gdp_df = gdp_df.filter(gdp_df.indicator == "gdp_per_capita")\
        .drop("country_code")

    energy_df = spark.read.table("energy_trans_dev.silver.silver_ember_generation")
    renewable_df = energy_df.filter(energy_df.energy_category == "renewables")

    co2_df = spark.read.table("energy_trans_dev.silver.silver_ember_country_yearly").select("country_name", "country_code", "co2_intensity_gco2_per_kwh", "year")
    wealth_energy_df = renewable_df.alias("r").join(gdp_df.alias("g"), on=['country_name', 'year'], how="inner")

    wealth_vs_co2_intensity = wealth_energy_df.alias("w").join(co2_df.alias("c"), on=["country_code", "year"], how="inner")\
        .select("w.country_name", "w.country_code", "w.year", "w.value", "c.co2_intensity_gco2_per_kwh", "w.share_of_generation_pct")
    return wealth_vs_co2_intensity

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
    el_access_df = wb_df.filter(wb_df.indicator == "electricity_access")

    access_energy_df = renewable_df.alias("r").join(el_access_df.alias("el"), on=['country_name', 'year'], how="inner")\
        .select("r.country_name", "r.country_code", "el.year", "el.value", "r.share_of_generation_pct")

    return access_energy_df

@dp.table(
    name="gold_energy_generation_share"
)
def gold_energy_generation_share():
    # Energy generation: fuel_type, share_pct
    # Plot share of renewables by fuel type on choropleth map
    energy_df = spark.read.table("energy_trans_dev.silver.silver_ember_generation_monthly")\
        .select("country_name", "country_code", "date", "energy_source", "energy_category", "share_of_generation_pct")

    pivot_df = energy_df.groupBy("country_name", "country_code", "date") \
    .pivot("energy_source", [
        "Bioenergy", "Coal", "Gas", "Hydro", "Net imports", 
        "Nuclear", "Other fossil", "Other renewables", "Solar", "Wind"
    ]) \
    .agg(F.avg("share_of_generation_pct"))

    return pivot_df

@dp.table(
    name="gold_capacity_per_capita"
)
def gold_capacity_per_capita(): 
    # Energy generation: electricity_capacity
    # WB: population
    # Plot capacity per capita on choropleth map
    capacity_df = spark.read.table("energy_trans_dev.silver.silver_ember_capacity")\
        .withColumn("year", F.year(F.col("date")))\
        .groupBy("country_name", "country_code", "year").agg(F.sum("capacity_gw").alias("capacity_gw"))

    wb_df = spark.read.table("energy_trans_dev.silver.silver_worldbank")
    pop_df = wb_df.filter(wb_df.indicator == "population")

    capacity_pop_df = capacity_df.alias("c").join(pop_df.alias("p"), on=["country_name", "year"], how="inner")\
        .withColumn("capacity_per_capita", F.col("capacity_gw")/F.col("value"))\
        .select("c.country_name", "c.country_code", "c.year", "c.capacity_gw", "p.value", "capacity_per_capita")

    return capacity_pop_df

@dp.table(
    name="gold_coal_decline_rate"
)
def gold_coal_decline_rate(): 
    # coal_twh coal_share_pct coal_mom_delta coal_yoy_delta gdp_per_capita
    coal_generation_df = spark.read.table("energy_trans_dev.silver.silver_ember_generation")\
        .filter(F.col("energy_source") == "Coal")\
        .withColumn("year", F.col("year").cast("int"))\
        .select("country_code", "country_name", "year", "share_of_generation_pct")

    window_spec = Window.partitionBy("country_code").orderBy("year")
    coal_transition_df = coal_generation_df.withColumn(
        "prev_share_pct", F.lag("share_of_generation_pct").over(window_spec)
    ).withColumn(
        "coal_yoy_delta", F.col("share_of_generation_pct") - F.col("prev_share_pct")
    )

    return coal_transition_df

@dp.table(name="gold_growth_transition")
def gold_growth_transition():
    wb_df = spark.read.table("energy_trans_dev.silver.silver_worldbank")
    gdp_df = wb_df.filter(wb_df.indicator == "gdp_per_capita") \
        .select("country_name", "year", "value")

    window_spec = Window.partitionBy("country_name").orderBy("year")
    
    gdp_growth_df = gdp_df.withColumn(
        "prev_gdp", F.lag("value").over(window_spec)
    ).withColumn(
        "gdp_growth_pct", ((F.col("value") - F.col("prev_gdp")) / F.col("prev_gdp")) * 100
    )

    energy_df = spark.read.table("energy_trans_dev.silver.silver_ember_generation") \
        .filter(F.col("energy_category") == "renewables") \
        .select("country_name", "year", "share_of_generation_pct")

    renewable_growth_df = energy_df.withColumn(
        "prev_share", F.lag("share_of_generation_pct").over(window_spec)
    ).withColumn(
        "renewable_yoy_delta", F.col("share_of_generation_pct") - F.col("prev_share")
    )
    renewable_growth_df = renewable_growth_df.select("country_name", "year", "renewable_yoy_delta", "share_of_generation_pct")
        
    return gdp_growth_df.join(renewable_growth_df, on=["country_name", "year"], how="inner").fillna(0)
