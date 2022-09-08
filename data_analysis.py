import pandas as pd
df = pd.read_parquet('data/sample_data.parquet', engine='pyarrow')
print("Rows,Columns")
print(df.shape)
