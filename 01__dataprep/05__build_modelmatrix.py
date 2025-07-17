"""Build model matrix with B3 data"""
import pandas as pd 

# Path
data_fact_path = "01__dataprep/data_treated/fact/%s"
data_dim_path = "01__dataprep/data_treated/dim/%s"

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

# Choosing variable income
market_type_negotation_cd = (
    dim_df
    .query("market_type == 10")['negotiation_cd']
    .unique()
)

# Pivot to wide
price_table = (
    fact_df
    .query("negotiation_cd in @negotation_cd")
    .pipe(
        pd.pivot_table,
        index='time',
        values='price',
        columns='negotiation_cd',
    )
    .fillna(method='ffill')
    .copy()
)

# Filter variables with less than 5% missing values
perc_missing_values = (
    price_table
    .isna()
    .mean(axis=0)
    .sort_values()
)

perc_negotation_cd = list(
    perc_missing_values[perc_missing_values < 0.05]
    .index
)

filtered_price_table = (
    price_table
    .loc[:, perc_negotation_cd]
    .reset_index()
    .dropna()
)