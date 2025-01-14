# Databricks notebook source
from IPython.core.display import HTML

# Create an HTML button with a link to www.databricks.com
button_html = """
<button onclick="window.open('https://signup.databricks.com/?l=en-US&dbx_source=www', '_blank')"
        style="background-color: #4CAF50; color: white; border: none; padding: 10px 20px; text-align: center; text-decoration: none; font-size: 16px; border-radius: 5px; cursor: pointer;">
    Go to Databricks
</button>
"""

# Display the button in the Jupyter Notebook
HTML(button_html)

# COMMAND ----------

import pandas as pd

# COMMAND ----------

import requests

r = requests.get("https://www.imf.org/external/datamapper/api/v1/NGDP_RPCH")
raw = r.json()['values']
print(raw)

# COMMAND ----------

raw_df = pd.DataFrame.from_dict(raw['NGDP_RPCH'], orient='index')
raw_df

# COMMAND ----------

df = raw_df.reset_index().melt(id_vars=['index'], var_name = 'year', value_name='gdp_growth')

# COMMAND ----------

def create_continent_mapping():
    continents = {
        'ASIA': [
            'BTN', 'CHN', 'HKG', 'IND', 'IDN', 'IRN', 'ISR', 'JPN', 'KOR', 'LAO', 
            'MDV', 'MNG', 'MMR', 'NPL', 'PAK', 'PHL', 'SGP', 'LKA', 'SYR', 'TWN', 
            'THA', 'VNM', 'BGD', 'BRN', 'KHM', 'MAC', 'MYS', 'TLS', 'AFG', 'IRQ',
            'YEM', 'BHR', 'KWT', 'OMN', 'QAT', 'SAU', 'ARE', 'JOR', 'LBN'
        ],
        'EUROPE': [
            'ALB', 'AUT', 'BEL', 'BGR', 'HRV', 'CYP', 'CZE', 'DNK', 'EST', 'FIN',
            'FRA', 'DEU', 'GRC', 'HUN', 'ISL', 'IRL', 'ITA', 'LVA', 'LTU', 'LUX',
            'MLT', 'NLD', 'NOR', 'POL', 'PRT', 'ROU', 'SVK', 'SVN', 'ESP', 'SWE',
            'CHE', 'GBR', 'BLR', 'MDA', 'RUS', 'UKR', 'AND', 'SMR', 'SRB', 'MNE',
            'BIH', 'MKD'
        ],
        'AFRICA': [
            'DZA', 'AGO', 'BEN', 'BWA', 'BFA', 'BDI', 'CMR', 'CPV', 'CAF', 'TCD',
            'COM', 'COG', 'COD', 'CIV', 'DJI', 'EGY', 'GNQ', 'ERI', 'ETH', 'GAB',
            'GMB', 'GHA', 'GIN', 'GNB', 'KEN', 'LSO', 'LBR', 'LBY', 'MDG', 'MWI',
            'MLI', 'MRT', 'MUS', 'MAR', 'MOZ', 'NAM', 'NER', 'NGA', 'RWA', 'STP',
            'SEN', 'SYC', 'SLE', 'SOM', 'ZAF', 'SSD', 'SDN', 'SWZ', 'TZA', 'TGO',
            'TUN', 'UGA', 'ZMB', 'ZWE'
        ],
        'NORTH_AMERICA': [
            'CAN', 'USA', 'MEX', 'BHS', 'BRB', 'CRI', 'CUB', 'DOM', 'SLV', 'GTM',
            'HTI', 'HND', 'JAM', 'NIC', 'PAN', 'TTO', 'BLZ', 'GRD', 'KNA', 'VCT',
            'ATG', 'DMA', 'LCA', 'PRI', 'ABW'
        ],
        'SOUTH_AMERICA': [
            'ARG', 'BOL', 'BRA', 'CHL', 'COL', 'ECU', 'GUY', 'PRY', 'PER', 'SUR',
            'URY', 'VEN'
        ],
        'OCEANIA': [
            'AUS', 'FJI', 'KIR', 'MHL', 'FSM', 'NRU', 'NZL', 'PLW', 'PNG', 'WSM',
            'SLB', 'TON', 'TUV', 'VUT'
        ]
    }
    
    # Create reverse mapping (country to continent)
    country_to_continent = {}
    for continent, countries in continents.items():
        for country in countries:
            country_to_continent[country] = continent
            
    # Add special cases/regions
    special_regions = {
        'WEOWORLD': 'WORLD',
        'EU': 'EUROPE',
        'EURO': 'EUROPE',
        'WE': 'EUROPE',
        'ADVEC': 'ADVANCED_ECONOMIES',
        'OEMDC': 'EMERGING_MARKETS',
        # Add other special cases as needed
    }
    country_to_continent.update(special_regions)
    
    return country_to_continent

# COMMAND ----------

create_continent_mapping()

# COMMAND ----------

df['continent'] = df['index'].map(create_continent_mapping())
df

# COMMAND ----------

display(df[df['year'] == '2024'])

# COMMAND ----------

display(df[df['year']<= '2025'])

# COMMAND ----------

display(df[(df['continent'] == 'EUROPE') & (df['year'] <= '2024')])

# COMMAND ----------

df[df['continent'] == 'ADVANCED_ECONOMIES']

# COMMAND ----------

import datetime

# COMMAND ----------

s = datetime.datetime.now()

# COMMAND ----------

 pd.DataFrame.from_dict(raw['NGDP_RPCH'])

# COMMAND ----------

e = datetime.datetime.now()

# COMMAND ----------

e - s

# COMMAND ----------


