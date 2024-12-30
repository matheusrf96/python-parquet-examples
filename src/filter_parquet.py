import pyarrow.parquet as pq
import pyarrow.compute as pc

# Filters: List of tuples
# Filter Tuple (key, op, value): pos[0] == "column name", pos[1] == "operator", pos[2] == "value"  # noqa
filters = [
    ("period", "==", "2024.03"),
    ("data_value", "<", "0"),
    ("status", "!=", "R"),
]

FILE_PATH = "../files/example.parquet"
table = pq.read_table(FILE_PATH, filters=filters)

# Match strings against SQL-style LIKE pattern
filtered_table = table.filter(pc.match_like(table["series_id"], "BOPQ.S06AD0000000%"))
filtered_df = filtered_table.to_pandas()

print(filtered_df)
print()
print(filtered_df.to_dict("records"))
