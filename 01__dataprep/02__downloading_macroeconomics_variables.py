"""Download Bacen macroeconomics variables time series."""

import pandas as pd
import requests
from datetime import datetime

# Path 
results_path = "01__dataprep/data_treated/%s"
url_dict = {
    "selic": \
        "https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados?formato=json",
    "ibovespa": \
        "https://api.bcb.gov.br/dados/serie/bcdata.sgs.7/dados?formato=json"
}

# Download data
bacen_lst = []
for key in url_dict.keys():
    url = url_dict.get(key)
    
    response = requests.get(url)
    data = response.json()
    bacen_dfi = pd.DataFrame(data).assign(variable=key)
    bacen_lst.append(bacen_dfi)

bacen_df = pd.concat(bacen_lst)\
    .rename(columns={
        "data": "time",
        "valor": "value"
    })\
    .assign(
        time = lambda x: pd.to_datetime(x.time, dayfirst=True)
    )\
    .query("time >= '2010-01-01'")\
    .copy()

# Saving results
bacen_df.to_parquet(results_path % "bacen_macroeconomics_vars.parquet")