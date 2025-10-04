"""Download Bacen macroeconomics variables time series."""
from datetime import datetime, timedelta

import pandas as pd
import requests

# Arguments
initial_date = (datetime.today() - timedelta(days=365 * 10))
formated_initial_date = initial_date.strftime("%d/%m/%Y")

# Path
bcb_url = "https://api.bcb.gov.br/dados/serie/"
results_path = "01__dataprep/data_treated/%s"

url_dict = {
    "selic": (
        bcb_url +
        "bcdata.sgs.11/dados?formato=json" +
        "&dataInicial={initial_date}"
        .format(initial_date=formated_initial_date)
    ),
    "ibovespa": (
        bcb_url +
        "bcdata.sgs.7832/dados?formato=json" +
        "&dataInicial={initial_date}"
        .format(initial_date=formated_initial_date)
    )
}

# Download data
bacen_lst = []
for key in url_dict.keys():
    url = url_dict.get(key)

    response = requests.get(url)  # noqa: S113
    data = response.json()
    bacen_dfi = pd.DataFrame(data).assign(variable=key)
    bacen_lst.append(bacen_dfi)

bacen_df = (
    pd.concat(bacen_lst)
    .rename(columns={
        "data": "time",
        "valor": "value"
    })
    .assign(time=lambda x: pd.to_datetime(x.time, dayfirst=True))
    .copy()
)

# Saving results
bacen_df.to_parquet(results_path % "bacen_macroeconomics_vars.parquet")
