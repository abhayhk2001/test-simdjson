import simdjson
import polars as pl
import pandas as pd
from time import time
import json
list = [{
    "name": "sathiyajith",
    "rollno": 56,
    "cgpa": 8.6,
    "phonenumber": "9976770500"
}, {
    "name": "sathiyajith",
    "rollno": 56,
    "cgpa": 8.6,
    "phonenumber": "9976770500",
    'heelo': 1
}]

json_object = json.dumps(list, indent=4)
with open("extras\sample_json_test_data_1_modified.json", "w") as outfile:
    outfile.write(json_object)

start = time()
parser = simdjson.Parser()
doc = parser.load("extras\sample_json_test_data_2_modified.json", True)
df = pd.DataFrame(doc)
print(df)
print("Time taken for simdjson: ", time()-start)

start = time()
df2 = pl.read_json("extras\sample_json_test_data_2_modified.json")
print(df2)
print("Time taken for polars: ", time()-start)
