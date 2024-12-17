
import csv
from typing import Any, Dict

import pandas as pd


def retrieve_data_from_csv() -> Dict:
    data = {
        "year": [],
        "industry_aggregation": [],
        "industry_code": [],
        "industry_name": [],
        "units": [],
        "variable_code": [],
        "variable_name": [],
        "variable_category": [],
        "value": [],
    }

    print("reading csv lines")
    FILE_PATH = "../files/annual-enterprise-survey-2023-financial-year-provisional.csv"
    with open(FILE_PATH, newline="") as csvfile:
        spamreader = csv.reader(csvfile, delimiter=",", quotechar="\"")
        for i, row in enumerate(spamreader):
            if i == 0:
                continue

            data["year"].append(row[0])
            data["industry_aggregation"].append(row[1])
            data["industry_code"].append(row[2])
            data["industry_name"].append(row[3])
            data["units"].append(row[4])
            data["variable_code"].append(row[5])
            data["variable_name"].append(row[6])
            data["variable_category"].append(row[7])
            data["value"].append(row[8])

    return data


def convert_csv_data_into_pandas_dataframe() -> Any:
    print("converting csv data into pandas dataframe")
    data = retrieve_data_from_csv()
    return pd.DataFrame(data=data)


def convert_dataframe_into_parquet_file() -> None:
    print("creating parquet file")
    data = convert_csv_data_into_pandas_dataframe()
    data.to_parquet("../files/example.parquet", index=False)
    print("parquet file has been created successfully")


convert_dataframe_into_parquet_file()
