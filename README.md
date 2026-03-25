# Energy Transition, CO₂ Emissions, and Economic Development (2016–2023)

## Project Overview

This project analyzes the relationship between energy transition, CO₂ emissions, and economic development across countries from **2016 to 2023**.

The goal is to explore possible **environmental and social issues** related to energy production, renewable adoption, electricity access, and carbon emissions.  
The project combines data from:

- **Ember Energy API**
- **World Bank API**

All data collection, transformation, analysis, and visualization are managed in **Databricks** using **Python**.

---

## Main Research Question

How do countries balance **economic growth**, **energy transition**, and **CO₂ emissions**, and what environmental or social patterns can be observed from 2016 to 2023?

---

## Key Questions

The project aims to answer the following questions:

- Are wealthier countries greener?
- Does higher energy demand lead to higher CO₂ emissions?
- Which countries are more energy-efficient?
- Does energy consumption grow together with population?
- Which countries are reducing coal usage the fastest?
- Do countries with high GDP transition to renewables faster?
- Does economic growth increase emissions, or can growth happen together with cleaner energy?

---

## Data Sources

### 1. Ember API
Used for electricity and power-sector related indicators, such as:

- renewable share in electricity generation
- non-renewable share
- electricity demand
- power sector emissions
- carbon intensity
- electricity generation by source
- coal generation / coal usage trends
- capacity per capita by source

### 2. World Bank API
Used for economic, environmental, and social indicators.

#### Selected indicators

| Simple name | World Bank code | Description |
|------------|-----------------|-------------|
| energy_use_per_capita | EG.USE.PCAP.KG.OE | Energy use per person (kg of oil equivalent) |
| renewables_share | EG.FEC.RNEW.ZS | Share of renewable energy in final energy consumption |
| electricity_access | EG.ELC.ACCS.ZS | % of population with access to electricity |
| co2_per_capita | EN.ATM.CO2E.PC | CO₂ emissions per person |
| co2_total | EN.ATM.CO2E.KT | Total CO₂ emissions (kilotons) |
| gdp_per_capita | NY.GDP.PCAP.CD | GDP per capita (current US$) |
| population | SP.POP.TOTL | Total population |
| gdp_growth | NY.GDP.MKTP.KD.ZG | Annual GDP growth (%) |

---

## Important Data Note

Some renewable indicators from Ember and World Bank measure different things:

- **World Bank renewables_share (EG.FEC.RNEW.ZS)**  
  Share of renewables in **total final energy consumption**  
  (includes transport, heating, industry, etc.)

- **Ember renewable share**  
  Share of renewables in **electricity generation only**

These indicators are **not equivalent**, so they should not be directly compared without explanation.

---

## Project Objectives

This project is focused on identifying patterns related to:

- transition away from coal
- adoption of renewable energy
- electricity accessibility
- emissions intensity
- links between economic development and cleaner energy

The broader purpose is to understand whether countries are moving toward a more sustainable energy system, and whether this transition happens equally across different income and development levels.

---

## Planned Analysis

### 1. Energy vs Wealth
Explore the relationship between a country’s economic level and energy greenness.

**Metrics:**
- GDP per capita
- renewable share
- non-renewable share

**Visualization:**
- Scatterplot
- Possible country categories:
  - high income / high renewable
  - high income / low renewable
  - low income / high renewable
  - low income / low renewable

---

### 2. Renewables Access Gap
Analyze electricity access compared to renewable share.

**Metrics:**
- electricity access
- renewable share

**Visualization:**
- Scatterplot

**Purpose:**
To examine whether countries with high renewable shares also provide broad electricity access, and to highlight possible off-grid or unequal renewable deployment.

---

### 3. Fuel Type Percentages
Show the percentage distribution of major electricity generation fuel types.

**Metrics:**
- coal
- gas
- hydro
- solar
- wind
- nuclear
- other renewables / fossil sources

**Visualization:**
- stacked bar chart or choropleth map (depending on final implementation)

---

### 4. Capacity per Capita
Compare countries by installed energy capacity per person.

**Metrics:**
- generation capacity per capita from Ember

**Visualization:**
- Choropleth map

**Purpose:**
To identify countries with stronger or weaker infrastructure for electricity generation.

---

### 5. Coal Energy Decline
Track which countries are moving away from coal.

**Metrics:**
- GDP
- change in coal usage / coal generation over time

**Visualization:**
- Bar chart
- N fastest countries by coal decline
- M slowest countries or countries still growing coal usage

---

### 6. Economic Growth vs Renewable Transition
Study whether economically growing countries prioritize cleaner energy.

**Metrics:**
- GDP growth
- renewable share growth
- fossil share / coal dependence

**Visualization:**
- Bar chart

**Purpose:**
To see whether rapid development supports green transition or continues dependence on emissions-intensive sources.

---

### 7. Energy Demand vs CO₂ Emissions
Check whether higher energy demand corresponds to higher emissions.

**Metrics:**
- electricity demand
- total CO₂ emissions
- CO₂ per capita

**Visualization:**
- Scatterplot

---

### 8. GDP vs CO₂ Intensity
Measure how efficiently countries generate economic value relative to emissions.

**Metrics:**
- GDP / GDP per capita
- carbon intensity
- CO₂ per capita

**Visualization:**
- Scatterplot

**Purpose:**
To identify relatively efficient and inefficient countries.

---

### 9. Population vs Energy Demand
Study whether larger populations necessarily consume more electricity.

**Metrics:**
- population
- electricity demand

**Visualization:**
- Scatterplot

---

## Environmental and Social Relevance

This project addresses several environmental and social issues:

### Environmental issues
- dependence on coal and fossil fuels
- high CO₂ emissions
- slow renewable transition
- high carbon intensity in electricity generation

### Social issues
- inequality in electricity access
- uneven renewable infrastructure
- possible gap between renewable progress and public energy availability
- development pressure in fast-growing economies

---

## Technology Stack

The project is developed and managed in **Databricks** using **Python**.

### Tools and Libraries
- Databricks notebooks
- Python
- PySpark / pandas
- requests
- matplotlib
- seaborn / plotly (optional, depending on final charts)
- World Bank API
- Ember API

---

## Project Workflow

The project is expected to follow this workflow:

1. **Data Collection**
   - Load country-level data from Ember API
   - Load country-level indicators from World Bank API

2. **Data Cleaning**
   - standardize country names and country codes
   - filter years from 2016 to 2023
   - handle missing values
   - align indicators from multiple sources

3. **Data Transformation**
   - calculate renewable and non-renewable shares
   - derive coal decline metrics
   - merge GDP, emissions, population, and energy indicators
   - create analysis-ready tables

4. **Exploratory Analysis**
   - compare countries
   - identify trends
   - group countries by income / transition profile

5. **Visualization**
   - create scatterplots, bar charts, and maps
   - highlight top / bottom performers
   - interpret environmental and social patterns

6. **Conclusion**
   - summarize major findings
   - discuss limitations
   - suggest future extensions
