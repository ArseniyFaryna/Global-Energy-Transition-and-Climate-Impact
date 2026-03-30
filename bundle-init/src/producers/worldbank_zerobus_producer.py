from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from zerobus.sdk.sync import ZerobusSdk
from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties
import requests
import time
from datetime import datetime, timezone

# ── Spark + DBUtils ───────────────────────────────────
spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)


# ── Zerobus credentials ───────────────────────────────
SERVER_ENDPOINT = "https://7405605185991044.zerobus.westeurope.azuredatabricks.net" 
WORKSPACE_URL   = "https://adb-7405605185991044.4.azuredatabricks.net"     
CLIENT_ID       = dbutils.secrets.get(scope="energy-kv", key="zb-client-id-energy")
CLIENT_SECRET   = dbutils.secrets.get(scope="energy-kv", key="zb-client-secret-energy")
TARGET_TABLE    = "energy_trans_dev.bronze.bronze_worldbank_zerobus" 

# ── Config ────────────────────────────────────────────
WB_INDICATORS = {
    "EG.ELC.ACCS.ZS": "electricity_access",       # % of the population with access to electricity
    "EG.USE.PCAP.KG.OE": "energy_use_per_capita", # Energy consumption per capita (kg of oil equivalent)
    "EG.FEC.RNEW.ZS": "renewables_share",         # % of renewable energy in final consumption
    "NY.GDP.MKTP.CD": "gdp",
    "NY.GDP.PCAP.CD": "gdp_per_capita",
    "SP.POP.TOTL": "population"
}

COUNTRIES = {
    "US": "United States",
    "CN": "China",
    "IN": "India",
    "DE": "Germany",
    "FR": "France",
    "GB": "United Kingdom",
    "IT": "Italy",
    "ES": "Spain",
    "PL": "Poland",
    "UA": "Ukraine",
    "CA": "Canada",
    "BR": "Brazil",
    "MX": "Mexico",
    "JP": "Japan",
    "KR": "South Korea",
    "AU": "Australia",
    "ID": "Indonesia",
    "TR": "Türkiye",
    "NL": "Netherlands",
    "BE": "Belgium",
    "SE": "Sweden",
    "NO": "Norway",
    "DK": "Denmark",
    "FI": "Finland",
    "CZ": "Czechia",
    "AT": "Austria",
    "RO": "Romania",
    "PT": "Portugal",
    "GR": "Greece",
    "ZA": "South Africa"
}

START_YEAR = 2015
END_YEAR   = 2026

WB_BASE_URL = "https://api.worldbank.org/v2"

all_rows = []
ingest_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

session = requests.Session()

def fetch_wb_batch(indicator, retries=3):
    countries_str = ";".join(COUNTRIES.keys())

    url = f"{WB_BASE_URL}/country/{countries_str}/indicator/{indicator}"

    params = {
        "format": "json",
        "date": f"{START_YEAR}:{END_YEAR}",
        "per_page": 2000
    }

    for attempt in range(retries):
        try:
            r = session.get(url, params=params, timeout=60)

            if r.status_code != 200:
                print(f"Error {r.status_code}")
                return []

            data = r.json()

            if isinstance(data, list) and len(data) > 1:
                return data[1]

            return []

        except Exception as e:
            print(f"Retry {attempt+1} error: {e}")
            time.sleep(2 * (attempt + 1))

    return []


for indicator_code, indicator_name in WB_INDICATORS.items():

    print(f"Fetching {indicator_code}")

    records = fetch_wb_batch(indicator_code)

    time.sleep(0.5)  

    for rec in records:
        if rec["value"] is None:
            continue

        all_rows.append({
            "country": rec["country"]["value"],   
            "country_code": rec["country"]["id"],
            "indicator": indicator_name,
            "indicator_code": indicator_code,
            "year": int(rec["date"]),
            "value": float(rec["value"]),
            "source": "worldbank",
            "ingested_at": ingest_at
        })


# ── Zerobus ingest ────────────────────────────────────
sdk = ZerobusSdk(SERVER_ENDPOINT, WORKSPACE_URL)

table_properties = TableProperties(TARGET_TABLE)
options = StreamConfigurationOptions(record_type=RecordType.JSON)

stream = sdk.create_stream(
    CLIENT_ID,
    CLIENT_SECRET,
    table_properties,
    options
)


try:
    # Batch ingest
    final_offset = stream.ingest_records_offset(all_rows)
    stream.flush()                  
    print(f"✅ {len(all_rows)} rows successfully saved to the {TARGET_TABLE} table")
    print(f"Final offset: {final_offset}")
    
except Exception as e:
    print(f"❌ Error in ingest: {e}")
    raise
finally:
    stream.close()

print("🎉 Іпgest World Bank data using Zerobus completed!")

