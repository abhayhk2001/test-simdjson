from pathlib import Path
import pandas as pd
from time import time

dir = './data/old_data'
pathlist = Path(dir).glob('**/*.parquet')
df = pd.DataFrame()
for path in pathlist:
    path = str(path)
    df1 = pd.read_parquet(path)
    df = pd.merge(df, df1, how='outer',
                  left_index=True, right_index=True)
df.to_parquet(dir + 'final.parquet')
