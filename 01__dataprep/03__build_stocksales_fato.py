"""Data fato tables for B3 dataset."""
import os
import requests
import urllib3
import pandas as pd
from zipfile import ZipFile
from tqdm import tqdm

# Path 
data_path = "01__dataprep/data_raw/%s"
results_path = "01__dataprep/data_treated/fato/%s"

# Loading data
column_names = [
    "Tipo Registro", 
    "Data Pregão", 
    "Código BDI", 
    "Código de Negociação", 
    "Tipo de Mercado", 
    "Nome da Empresa",
    "Especificação do Papel", 
    "Prazo em Dias", 
    "Moeda", 
    "Preço de Abertura", 
    "Preço Máximo", 
    "Preço Mínimo",
    "Preço Médio", 
    "Preço de Último Negócio", 
    "Preço de Melhor Oferta de Compra", 
    "Preço de Melhor Oferta de Venda",
    "Número de Negócios", 
    "Quantidade de Títulos Negociados", 
    "Volume Financeiro", 
    "Fator de Cotação",
    "Preço de Exercício", 
    "Indicador de Correcão", 
    "Data de Vencimento", 
    "Código do Papel no Sistema ISIN", 
    "Número de Distribuição"
]

colspecs = [
    (0, 2), 
    (2, 10), 
    (10, 12), 
    (12, 24), 
    (24, 27), 
    (27, 39), 
    (39, 49), 
    (49, 52), 
    (52, 56), 
    (56, 69),
    (69, 82), 
    (82, 95), 
    (95, 108), 
    (108, 121), 
    (121, 134), 
    (134, 147), 
    (147, 152), 
    (152, 170), 
    (170, 188),
    (188, 201), 
    (201, 202), 
    (202, 216), 
    (216, 229), 
    (229, 242), 
    (242, 245)
]

start_year = 2010
end_year = 2025
for y in tqdm(range(start_year, end_year + 1)):
    b3_df = pd.read_fwf(data_path % "COTAHIST_A{year}.TXT".format(year=y),
                        colspecs=colspecs, 
                        names=column_names, 
                        encoding="latin-1"
                        )

    # Build Fato table
    filter_columns = [
        "Data Pregão", 
        "Código de Negociação", 
        "Código do Papel no Sistema ISIN", 
        "Preço de Abertura", 
        "Quantidade de Títulos Negociados"
    ]

    fato__b3_df = b3_df[filter_columns].copy()\
        .rename(columns={
            "Data Pregão": "time",
            "Código de Negociação": "negotiation_cd",
            "Código do Papel no Sistema ISIN": "isin_cd",
            "Preço de Abertura": "price",
            "Quantidade de Títulos Negociados": "quantity"
        }
    ).dropna()\
    .set_index(['negotiation_cd', 'isin_cd'])\
    .reset_index()

    # Saving results
    fato__b3_df.to_parquet(
        results_path % "fato__A{year}.parquet".format(year=y))