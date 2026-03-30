# Energy vs wealth 
@dp.table(
    name = "gold_energy_profile",
    description = "Energy vs wealth"
)
def gold_wealth_energy(): pass
    # Energy generation: pct of renewables,
    # CO2 intensity: co2_intensity
    # WB: gdp per capita (to plot the wealth axis), income_level (to categorise countries)
    # Plot gdp agains co2 intensity and color by renewables %


@dp.table(
    name="gold_access_gap"
)
def gold_access_gap(): pass
    # Electricity access: elec_access_pct
    # Energy generation: pct of renewables,
    # See how involved in national grid renewables resource are. 

@dp.table(
    name="gold_energy_generation_share"
)
def gold_energy_generation_share():pass
    # Energy generation: fuel_type, share_pct
    # Plot share of renewables by fuel type on choropleth map

@dp.table(
    name="gold_capacity_per_capita"
)
def gold_capacity_per_capita(): pass
    # Energy generation: electricity_capacity
    # WB: population
    # Plot capacity per capita on choropleth map

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
def dim_fuel_types(): pass
    # series fuel_group is_renewable is_fossil

@dp.table(
    name="dim_country_codes"
)
def dim_country_codes(): pass
    # entity_code entity_name iso2 income_level wb_region is_aggregate_entity

