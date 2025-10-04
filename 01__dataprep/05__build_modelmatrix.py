"""Build model matrix with B3 data."""
import numpy as np
import pandas as pd
from datetime import datetime

# Path
data_fact_path = "01__dataprep/data_treated/fact/%s"
data_dim_path = "01__dataprep/data_treated/dim/%s"
results_path = "01__dataprep/data_treated/%s"

# Loading data
start_year = 2010
end_year = 2025
fact_lst = [
    pd.read_parquet(data_fact_path % "fact__A{year}.parquet".format(year=y))
    for y in range(start_year, end_year + 1)
]

dim_lst = [
    pd.read_parquet(data_dim_path % "dim__A{year}.parquet".format(year=y))
    for y in range(start_year, end_year + 1)
]

# Concat results
dim_df = pd.concat(dim_lst)
fact_df = pd.concat(fact_lst)

# Format time column
fact_df['time'] = pd.to_datetime(fact_df['time'])

# Choosing market type = 10 wich are associated to cash sales.
# The codes can be check in the following link
# https://www.b3.com.br/data/files/33/67/B9/50/
# D84057102C784E47AC094EA8/SeriesHistoricas_Layout.pdf
market_type_negotiation_cd = (
    dim_df
    .query("market_type == 10")['negotiation_cd']
    .unique()
)

# Exclude times with prices lower than zero.
filtered_fact_df = fact_df.loc[fact_df['price'] > 0.0].copy()

# Exclude assets with less than 4 years of observations.
nobs_per_negotiation_cd = (
    filtered_fact_df
    .groupby('negotiation_cd')
    .size()
)

nobs_selected_assets = (
    nobs_per_negotiation_cd
    .loc[nobs_per_negotiation_cd > 251 * 15]
    .index
    .tolist()
)

# Exclude assets with last observation time lesser
# than the current date minus 7 days
current_time = pd.Timestamp.today().normalize()

last_time_per_negotiation_df = (
    filtered_fact_df
    .reset_index()
    .groupby('negotiation_cd')[['time']]
    .max()
    .assign(
        time_diff=lambda x: (current_time - x['time']).dt.days
    )
    .copy()
)

time_selected_assets = (
    last_time_per_negotiation_df
    .loc[last_time_per_negotiation_df['time_diff'] <= 7]
    .index
    .tolist()
)

# Get the intersection of number of observations and time lists
intersection_lst = list(set(nobs_selected_assets) & set(time_selected_assets))

# Pivot to wide
price_table = (
    filtered_fact_df
    .loc[filtered_fact_df['negotiation_cd'].isin(intersection_lst)]
    .pipe(
        pd.pivot_table,
        index='time',
        values='price',
        columns='negotiation_cd',
    )
    .ffill()
    .dropna()
    .copy()
)

# Get percentual variation values
log_returns_df = np.log(price_table).diff().dropna().reset_index()

# Saving results
log_returns_df.to_parquet(
    results_path % "modelmatrix.parquet",
    index=False
)
