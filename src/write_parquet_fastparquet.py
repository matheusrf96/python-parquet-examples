
import csv
from typing import Any, Dict

import pandas as pd
from fastparquet import write


def retrieve_data_from_csv() -> Dict:
    data = {
        "series_id": [],
        "period": [],
        "data_value": [],
        "suppressed": [],
        "status": [],
        "units": [],
        "magnitude": [],
        "subject": [],
        "group": [],
        "series_title": [],
    }

    print("reading csv lines")
    FILE_PATH = "../files/balance-of-payments-and-international-investment-position-june-2024-quarter.csv"  # noqa
    with open(FILE_PATH, newline="") as csvfile:
        spamreader = csv.reader(csvfile, delimiter=",", quotechar="\"")
        for i, row in enumerate(spamreader):
            if i == 0:
                continue

            data["series_id"].append(row[0])
            data["period"].append(row[1])
            data["data_value"].append(row[2] or "0")
            data["suppressed"].append(True if row[3] else False)
            data["status"].append(row[4])
            data["units"].append(row[5])
            data["magnitude"].append(row[6])
            data["subject"].append(row[7])
            data["group"].append(row[8])
            data["series_title"].append(row[9])

    return data


def convert_csv_data_into_pandas_dataframe(data: Dict) -> Any:
    print("converting csv data into pandas dataframe")
    return pd.DataFrame(data=data)


def convert_dataframe_into_parquet_file(df: Any) -> None:
    FILE_PATH = "../files/example.parq"
    print("creating parquet file")
    write(
        filename=FILE_PATH,
        data=df,
        row_group_offsets=[0, 10000, 20000],
        compression="GZIP",
        file_scheme="hive",
    )
    print("parquet file has been created successfully")


csv_data = retrieve_data_from_csv()
dataframe = convert_csv_data_into_pandas_dataframe(data=csv_data)
convert_dataframe_into_parquet_file(df=dataframe)
