
import os

import pyarrow.parquet as pq


def describe_parquet(file_path: str) -> None:
    file_size = os.path.getsize(file_path)
    print(f"File Size: {file_size} bytes")

    table = pq.read_table(file_path)
    columns = table.column_names

    print(f"Number of rows: {table.num_rows}")
    print(f"Number of columns: {len(columns)}")

    print("Columns:")
    for column in columns:
        print(f"\t{column}")


FILE_PATH = "../files/example.parq"
describe_parquet(FILE_PATH)
