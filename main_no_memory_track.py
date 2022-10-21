# from distributed import Client
# client = Client(process=False)
import modin.pandas as pd
import shutil
import tracemalloc
import os
import gzip
from time import time
import pandas as pds
import fnmatch
from pathlib import Path
import simdjson


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


def merge_files(dir):
    if (not os.path.isdir(dir)):
        return
    if (len(fnmatch.filter(os.listdir(dir), '*.parquet')) == 1):
        return
    pathlist = Path(dir).glob('**/*.parquet')
    df = pd.DataFrame()
    for path in pathlist:
        path = str(path)
        df1 = pds.read_parquet(path)
        df = pd.merge(df, df1, how='outer',
                      left_index=True, right_index=True)
    df.to_parquet(dir + '/final.parquet')

def custom_algorithm():
    
    pass

def run(count):
    start = time()
    filename = format_file("sample_json_test_data_2.json", count)
    format_time = time() - start
    row.extend([format_time])

    parser = simdjson.Parser()
    doc = parser.load(filename, True)
    df = pd.DataFrame(doc)
    load_time = time() - format_time - start
    row.extend([load_time])

    df = df.pivot_table(index=['timestamp', 'device_uuid'],
                        columns='data_item_name', aggfunc='first')
    pivot_time = time() - load_time - start
    row.extend([pivot_time])

    df.to_parquet(f"{filename[:-5]}.parquet", engine="pyarrow")
    convert_time = time() - pivot_time - start
    row.extend([convert_time])

    row.append(time() - start)
    # merge_files(f"{filename[:-5]}.parquet")
    return (filename, f"{filename[:-5]}.parquet")


counts = [500, 1000, 5000, 10000, 50000, 100000, 124703]
counts = [500, 1000, 5000]

rows = []
for count in counts:
    row = []
    print(f"Running for count: {count}")
    tracemalloc.start()
    (fname, parquet_name) = run(count)
    tracemalloc.stop()
    print("\n\n")
    os.remove(fname)
    shutil.rmtree(parquet_name)
    rows.append(row)

col_name = ['Formatting', 'Loading File',
            'Pivotting Table', 'Converting Table to Parquet']
col_name_sub = ['Time']
cols = []
for i in col_name:
    for j in col_name_sub:
        cols.append((i, j))
cols.append(('Total',))
col_list = pd.MultiIndex.from_tuples(cols)
df = pd.DataFrame(rows, counts, col_list)
print(df)

df.to_excel("./results.xlsx")
