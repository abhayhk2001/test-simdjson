import simdjson
import polars as pl
import pandas as pd
from time import time

start = time()
parser = simdjson.Parser()
doc = parser.load("./extras/sample_json_test_data_2_modified.json", True)
df = pd.DataFrame(doc)
print("Time taken for simdjson: ", time()-start)

start = time()
df1 = pl.read_json("./extras/sample_json_test_data_2_modified.json")
print("Time taken for polars: ", time()-start)
