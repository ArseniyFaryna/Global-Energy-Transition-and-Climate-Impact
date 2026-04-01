-- Groups already created in workspace
-- europe_analysts:   kovalnasta919@gmail.com
-- americas_analysts: farynaarseniy2007@softserve.academy
-- external_clients:  tr3e1t0ry@softserve.academy
-- senior_analysts:   lbiel@softserve.academy

-- for running before deploy bundle
ALTER MATERIALIZED VIEW energy_trans_dev.gold.gold_access_gap DROP ROW FILTER;
ALTER MATERIALIZED VIEW energy_trans_dev.gold.gold_capacity_per_capita DROP ROW FILTER;
ALTER MATERIALIZED VIEW energy_trans_dev.gold.gold_coal_decline_rate DROP ROW FILTER;
ALTER MATERIALIZED VIEW energy_trans_dev.gold.gold_energy_generation_share DROP ROW FILTER;
ALTER MATERIALIZED VIEW energy_trans_dev.gold.gold_energy_profile DROP ROW FILTER;
ALTER MATERIALIZED VIEW energy_trans_dev.gold.gold_growth_transition DROP ROW FILTER;

-- RLS Function

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
    );

--RLS for Gold таблиц

-- gold_energy_profile
ALTER MATERIALIZED VIEW energy_trans_dev.gold.gold_access_gap
    SET ROW FILTER energy_trans_dev.governance.rls_region_filter
    ON (country_code);

-- gold_access_gap
ALTER MATERIALIZED VIEW energy_trans_dev.gold.gold_capacity_per_capita
    SET ROW FILTER energy_trans_dev.governance.rls_region_filter
    ON (country_code);

-- gold_energy_generation_share
ALTER MATERIALIZED VIEW energy_trans_dev.gold.gold_coal_decline_rate
    SET ROW FILTER energy_trans_dev.governance.rls_region_filter
    ON (country_code);

-- gold_capacity_per_capita
ALTER MATERIALIZED VIEW energy_trans_dev.gold.gold_energy_generation_share
    SET ROW FILTER energy_trans_dev.governance.rls_region_filter
    ON (country_code);

-- gold_coal_decline_rate
ALTER MATERIALIZED VIEW energy_trans_dev.gold.gold_energy_profile
    SET ROW FILTER energy_trans_dev.governance.rls_region_filter
    ON (country_code);

-- gold_coal_decline_rate
ALTER MATERIALIZED VIEW energy_trans_dev.gold.gold_growth_transition
    SET ROW FILTER energy_trans_dev.governance.rls_region_filter ON (country_name);

-- CLS Function

-- Mask 1: NULL for external_clients 
CREATE OR REPLACE FUNCTION
    energy_trans_dev.governance.mask_sensitive_double(value DOUBLE)
RETURNS DOUBLE
RETURN
    CASE
      WHEN is_account_group_member('senior_analysts')
        OR is_account_group_member('europe_analysts')
        OR is_account_group_member('americas_analysts')
        THEN value
        ELSE NULL
    END;


-- Mask 2: rounding to the nearest thousand for external_clients (GDP)
CREATE OR REPLACE FUNCTION
    energy_trans_dev.governance.mask_gdp_double(value DOUBLE)
RETURNS DOUBLE
RETURN
    CASE
      WHEN is_account_group_member('senior_analysts')
        OR is_account_group_member('europe_analysts')
        OR is_account_group_member('americas_analysts')
        THEN value
        ELSE ROUND(value, -3)
    END;

-- Mask 3: rounding to the nearest whole percentage point for external_clients
CREATE OR REPLACE FUNCTION
    energy_trans_dev.governance.mask_share_pct_double(value DOUBLE)
RETURNS DOUBLE
RETURN
    CASE
      WHEN is_account_group_member('senior_analysts')
        OR is_account_group_member('europe_analysts')
        OR is_account_group_member('americas_analysts')
        THEN value
        ELSE ROUND(value, 0)
    END;

-- gold_energy_profile
ALTER MATERIALIZED VIEW energy_trans_dev.gold.gold_energy_profile
    ALTER COLUMN co2_intensity_gco2_per_kwh
    SET MASK energy_trans_dev.governance.mask_sensitive_double;

ALTER MATERIALIZED VIEW energy_trans_dev.gold.gold_energy_profile
    ALTER COLUMN value
    SET MASK energy_trans_dev.governance.mask_gdp_double;

ALTER MATERIALIZED VIEW energy_trans_dev.gold.gold_energy_profile
    ALTER COLUMN share_of_generation_pct
    SET MASK energy_trans_dev.governance.mask_share_pct_double;
