import simdjson
import pandas as pd
from datetime import datetime as dt
from time import time
import gzip
import os


def format_file(filename, count):
    fw = open(f"./data/{filename[:-5] + '_modified.json'}", "wb")
    fw.write("[".encode('ascii'))
    with gzip.open(f'./data/{filename}.gz', 'rb') as fr:
        prev_line = None
        for line in fr:
            if (prev_line is not None):
                fw.write((prev_line + ",".encode('ascii')))
                count -= 1
            prev_line = line
            if count == 1:
                break
        fw.write(prev_line)
    fw.write("]".encode('ascii'))
    fr.close()
    fw.close()
    return f"./data/{filename[:-5] + '_modified.json'}"


def run(count):
    start = time()
    filename = format_file("sample_json_test_data_2.json", count)
    format_time = time() - start
    print(f"Time taken to format file: {format_time}")
    parser = simdjson.Parser()
    doc = parser.load(filename, True)
    df = pd.DataFrame(doc)
    load_time = time() - format_time - start
    print("Time taken to load file: ", load_time)
    # df['timestamp'] = df['timestamp'].apply(lambda x: dt.fromtimestamp(x/1000))
    df = df.pivot_table(index=['timestamp', 'device_uuid'],
                        columns='data_item_name', aggfunc='first')
    # df.columns.set_levels([""], level=0, inplace=True)
    # df.columns = df.columns.droplevel(0)
    pivot_time = time() - load_time - start
    print("Time taken to pivot table: ", pivot_time)

    df.to_parquet(f"{filename[:-5]}.parquet", engine="pyarrow")

    convert_time = time() - pivot_time - start
    print("Time taken to convert to parquet: ", convert_time)
    print(f"Total time taken: {time() - start}")
    return (filename, f"{filename[:-5]}.parquet")


counts = [500, 1000, 5000, 10000, 50000, 100000, 124703]
for count in counts:
    print(f"Running for count: {count}")
    (fname, parquet_name) = run(count)
    print("\n\n")
    os.remove(fname)
    os.remove(parquet_name)
