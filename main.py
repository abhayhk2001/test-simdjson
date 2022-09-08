import simdjson
import pandas as pd

parser = simdjson.Parser()
doc = parser.load("data/sample_data2.json", True)

df = pd.DataFrame(doc)


df['timestamp'] = pd.to_datetime(df['timestamp'])
# df['timestamp'] = df['timestamp'].astype('datetime64[ns]')
df['timestamp'] = df['timestamp'].astype('datetime64[us]')

df = df.pivot_table(index=['timestamp', 'device_uuid'],
                    columns='data_item_name', aggfunc='first')
# df.columns.set_levels([None], level=0, inplace=True)
df.columns.set_levels([""], level=0, inplace=True)


df.to_parquet("data/sample_data2.parquet", engine='pyarrow')
# change Columns names by reading from parquet and then writing back
print(df)
