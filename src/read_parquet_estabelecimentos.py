
import pandas as pd


def read_parquet_data():
    FILE_PATH = "../files/estabelecimentos/Estabelecimentos1.parquet"
    df = pd.read_parquet(FILE_PATH)

    print("Contents of the Parquet file:")
    print(df)


read_parquet_data()
