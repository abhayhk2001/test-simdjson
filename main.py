import simdjson
import pandas as pd

parser = simdjson.Parser()
doc = parser.load("sample_data2.json", True)

df = pd.DataFrame(doc)
df.to_parquet("sample_data2.parquet")

print(df)
