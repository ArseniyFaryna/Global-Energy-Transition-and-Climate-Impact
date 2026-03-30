# Imports
import dlt
import requests
import json
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import current_timestamp, lit

# Variable decalration
BASE_URL = "https://api.ember-energy.org/v1"
EMBER_ENERGY_API_KEY = dbutils.secrets.get("tokariev-scope", "ember-api-key")
END_DATE = datetime.now().strftime("%Y-%m")
START_DATE = (datetime.now() - relativedelta(months=12)).strftime("%Y-01")
START_YEAR = datetime.now().year - 1

# Fetch function
def fetch_ember(path: str):
    """Fetch data from the Ember Energy API and return a Spark DataFrame."""
    start_time = START_DATE if "/monthly" in path else START_YEAR

    params = {
        "is_aggregate_entity": False,
        "start_date": start_time,
        "end_date": END_DATE,
        "api_key": EMBER_ENERGY_API_KEY,
    }

    response = requests.get(f"{BASE_URL}/{path}", params=params)
    response.raise_for_status()

    data = response.json().get("data")
    pdf = pd.DataFrame(data)
    df = spark.createDataFrame(pdf)

    return df.withColumn("_ingested_at", current_timestamp()) \
             .withColumn("_source", lit(path))

# Electricity Generation (Monthly)
@dlt.table(
    name="bronze_electricity_generation_monthly"
)
def bronze_electricity_generation_monthly():
    return fetch_ember("electricity-generation/monthly")

# Electricity Generation (Yearly)
@dlt.table(
    name="bronze_electricity_generation_yearly"
)
def bronze_electricity_generation_yearly():
    return fetch_ember("electricity-generation/yearly")

# Installed capacity (Monthly)
@dlt.table(
    name="bronze_installed_capacity_monthly"
)
def bronze_installed_capacity_monthly():
    return fetch_ember("installed-capacity/monthly")

# Power sector emissions (Monthly)
@dlt.table(
    name="bronze_power_sector_emissions_monthly"
)
def bronze_power_sector_emissions_monthly():
    return fetch_ember("power-sector-emissions/monthly")
# Power sector emissions (Yearly)
@dlt.table(
    name="bronze_power_sector_emissions_yearly"
)
def bronze_power_sector_emissions_yearly():
    return fetch_ember("power-sector-emissions/yearly")

# Carbon intensity (Monthly)
@dlt.table(
    name="bronze_carbon_intensity_monthly"
)
def bronze_carbon_intensity_monthly():
    return fetch_ember("carbon-intensity/monthly")

# Carbon intensity (Yearly)
@dlt.table(
    name="bronze_carbon_intensity_yearly"
)
def bronze_carbon_intensity_yearly():
    return fetch_ember("carbon-intensity/yearly")

# Elcetricity demand (Monthly)
@dlt.table(
    name="bronze_electricity_demand_monthly"
)
def bronze_electricity_demand_monthly():
    return fetch_ember("electricity-demand/monthly")

# Elcetricity demand (Yearly)
@dlt.table(
    name="bronze_electricity_demand_yearly"
)
def bronze_electricity_demand_yearly():
    return fetch_ember("electricity-demand/yearly")



