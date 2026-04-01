# Groups already created in workspace
#  europe_analysts:   kovalnasta919@gmail.com
#  americas_analysts: farynaarseniy2007@softserve.academy
#  senior_analysts:   lbiel@softserve.academy

# Drop existing filters
tables = [
    "gold_access_gap",
    "gold_capacity_per_capita", 
    "gold_coal_decline_rate",
    "gold_energy_generation_share",
    "gold_energy_profile",
    "gold_growth_transition",
]

for table in tables:
    spark.sql(f"""
        ALTER MATERIALIZED VIEW energy_trans_dev.gold.{table}
        DROP ROW FILTER
    """)
    print(f"Dropped filter: {table}")

# RLS Function
spark.sql("""
    CREATE OR REPLACE FUNCTION
        energy_trans_dev.governance.rls_region_filter(country_code STRING)
    RETURNS BOOLEAN
    RETURN
        IS_ACCOUNT_GROUP_MEMBER('senior_analysts')
        OR
        IS_ACCOUNT_GROUP_MEMBER('external_clients')
        OR
        EXISTS (
            SELECT 1
            FROM energy_trans_dev.governance.country_region_access r
            WHERE r.country_code = rls_region_filter.country_code
            AND IS_ACCOUNT_GROUP_MEMBER(r.analyst_group)
        )
""")
print("✅ RLS function created")

# Apply RLS
for table in tables:
    spark.sql(f"""
        ALTER MATERIALIZED VIEW energy_trans_dev.gold.{table}
            SET ROW FILTER energy_trans_dev.governance.rls_region_filter
            ON (country_code)
    """)
    print(f"✅ RLS applied: {table}")

# CLS Functions
spark.sql("""
    CREATE OR REPLACE FUNCTION
        energy_trans_dev.governance.mask_sensitive_double(value DOUBLE)
    RETURNS DOUBLE
    RETURN CASE
        WHEN IS_ACCOUNT_GROUP_MEMBER('senior_analysts')   THEN value
        WHEN IS_ACCOUNT_GROUP_MEMBER('europe_analysts')   THEN value
        WHEN IS_ACCOUNT_GROUP_MEMBER('americas_analysts') THEN value
        WHEN IS_ACCOUNT_GROUP_MEMBER('apac_analysts')     THEN value
        WHEN IS_ACCOUNT_GROUP_MEMBER('africa_analysts')   THEN value
        ELSE NULL
    END
""")

spark.sql("""
    CREATE OR REPLACE FUNCTION
        energy_trans_dev.governance.mask_gdp_double(value DOUBLE)
    RETURNS DOUBLE
    RETURN CASE
        WHEN IS_ACCOUNT_GROUP_MEMBER('senior_analysts')   THEN value
        WHEN IS_ACCOUNT_GROUP_MEMBER('europe_analysts')   THEN value
        WHEN IS_ACCOUNT_GROUP_MEMBER('americas_analysts') THEN value
        WHEN IS_ACCOUNT_GROUP_MEMBER('apac_analysts')     THEN value
        WHEN IS_ACCOUNT_GROUP_MEMBER('africa_analysts')   THEN value
        ELSE ROUND(value, -3)
    END
""")

spark.sql("""
    CREATE OR REPLACE FUNCTION
        energy_trans_dev.governance.mask_share_pct_double(value DOUBLE)
    RETURNS DOUBLE
    RETURN CASE
        WHEN IS_ACCOUNT_GROUP_MEMBER('senior_analysts')   THEN value
        WHEN IS_ACCOUNT_GROUP_MEMBER('europe_analysts')   THEN value
        WHEN IS_ACCOUNT_GROUP_MEMBER('americas_analysts') THEN value
        WHEN IS_ACCOUNT_GROUP_MEMBER('apac_analysts')     THEN value
        WHEN IS_ACCOUNT_GROUP_MEMBER('africa_analysts')   THEN value
        ELSE ROUND(value, 0)
    END
""")

spark.sql("""
    CREATE OR REPLACE FUNCTION
        energy_trans_dev.governance.mask_delta_double(value DOUBLE)
    RETURNS DOUBLE
    RETURN CASE
        WHEN IS_ACCOUNT_GROUP_MEMBER('senior_analysts')   THEN value
        WHEN IS_ACCOUNT_GROUP_MEMBER('europe_analysts')   THEN value
        WHEN IS_ACCOUNT_GROUP_MEMBER('americas_analysts') THEN value
        WHEN IS_ACCOUNT_GROUP_MEMBER('apac_analysts')     THEN value
        WHEN IS_ACCOUNT_GROUP_MEMBER('africa_analysts')   THEN value
        ELSE ROUND(value, 1)
    END
""")

print("✅ Masking functions created")

# Apply masks
spark.sql("""ALTER MATERIALIZED VIEW energy_trans_dev.gold.gold_energy_profile
    ALTER COLUMN co2_intensity_gco2_per_kwh
    SET MASK energy_trans_dev.governance.mask_sensitive_double""")

spark.sql("""ALTER MATERIALIZED VIEW energy_trans_dev.gold.gold_energy_profile
    ALTER COLUMN value
    SET MASK energy_trans_dev.governance.mask_gdp_double""")

spark.sql("""ALTER MATERIALIZED VIEW energy_trans_dev.gold.gold_energy_profile
    ALTER COLUMN share_of_generation_pct
    SET MASK energy_trans_dev.governance.mask_share_pct_double""")

spark.sql("""ALTER MATERIALIZED VIEW energy_trans_dev.gold.gold_energy_generation_share
    ALTER COLUMN share_of_generation_pct
    SET MASK energy_trans_dev.governance.mask_share_pct_double""")

spark.sql("""ALTER MATERIALIZED VIEW energy_trans_dev.gold.gold_coal_decline_rate
    ALTER COLUMN coal_yoy_delta
    SET MASK energy_trans_dev.governance.mask_delta_double""")

spark.sql("""ALTER MATERIALIZED VIEW energy_trans_dev.gold.gold_growth_transition
    ALTER COLUMN gdp_growth_pct
    SET MASK energy_trans_dev.governance.mask_delta_double""")

spark.sql("""ALTER MATERIALIZED VIEW energy_trans_dev.gold.gold_capacity_per_capita
    ALTER COLUMN capacity_per_capita
    SET MASK energy_trans_dev.governance.mask_share_pct_double""")

print("✅ All column masks applied")
print("🎉 RLS + CLS setup completed!")