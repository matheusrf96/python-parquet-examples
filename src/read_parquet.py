
import pandas as pd


def read_parquet_data():
    FILE_PATH = "../files/example.parquet"
    df = pd.read_parquet(FILE_PATH)

    print("Contents of the Parquet file:")
    print(df)


read_parquet_data()
