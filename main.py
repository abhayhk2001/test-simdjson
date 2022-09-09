import simdjson
import pandas as pd
from datetime import datetime as dt

parser = simdjson.Parser()
doc = parser.load("data/sample_data2.json", True)
df = pd.DataFrame(doc)

df['timestamp'] = df['timestamp'].apply(lambda x: dt.fromtimestamp(x/1000))
df = df.pivot_table(index=['timestamp', 'device_uuid'],
                    columns='data_item_name', aggfunc='first')
df.columns.set_levels([""], level=0, inplace=True)

df.to_parquet("data/sample_data2.parquet", engine="pyarrow")
print(df)
