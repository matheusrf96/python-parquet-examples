import pyarrow.parquet as pq


# Filters: List of tuples
# Filter Tuple: pos[0] == "column name", pos[1] == "operator", pos[2] == "value"
filters = [
    ('period', '=', '2024.03'),
    ("data_value", "<", "0"),
    ("status", "!=", "R"),
]

table = pq.read_table('../files/example.parquet', filters=filters)
filtered_df = table.to_pandas()

print(filtered_df)
