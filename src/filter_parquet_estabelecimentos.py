import pyarrow.parquet as pq

# Filters: List of tuples
# Filter Tuple (key, op, value): pos[0] == "column name", pos[1] == "operator", pos[2] == "value"  # noqa
filters = [
    ("cnpj_1", "==", "64904295"),
]

FILE_PATH = "../files/estabelecimentos/Estabelecimentos1.parquet"
table = pq.read_table(FILE_PATH, filters=filters)
filtered_df = table.to_pandas()

print(filtered_df)
print()
print(filtered_df.to_dict("records"))
