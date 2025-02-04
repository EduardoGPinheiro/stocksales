"""Build model matrix with B3 data"""
import pandas as pd 

# Path
data_path = "01__dataprep/data_treated/fato/%s"

# Loading data
start_year = 2010
end_year = 2025
fato_lst = [
    pd.read_parquet(data_path % "fato__A{year}.parquet".format(year=y))
    for y in range(start_year, end_year + 1)
]

# Concat results
concat_fato_df = pd.concat(fato_lst)

# Pivot to wide
wide_fato_df = pd.pivot(
    concat_fato_df, 
    index=['time', 'negotiation_cd', 'isin_cd'],
    value='price'
    )