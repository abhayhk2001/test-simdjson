import simdjson
import modin.pandas as pd
from time import time
import gzip
import os
import tracemalloc
import shutil


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
    current, peak = tracemalloc.get_traced_memory()
    row.extend([format_time, current / 10**6, peak / 10**6])

    parser = simdjson.Parser()
    doc = parser.load(filename, True)
    df = pd.DataFrame(doc)
    load_time = time() - format_time - start
    current, peak = tracemalloc.get_traced_memory()
    row.extend([load_time, current / 10**6, peak / 10**6])

    df = df.pivot_table(index=['timestamp', 'device_uuid'],
                        columns='data_item_name', aggfunc='first')
    pivot_time = time() - load_time - start
    current, peak = tracemalloc.get_traced_memory()
    row.extend([pivot_time, current / 10**6, peak / 10**6])

    df.to_parquet(f"{filename[:-5]}.parquet", engine="pyarrow")
    convert_time = time() - pivot_time - start
    current, peak = tracemalloc.get_traced_memory()
    row.extend([convert_time, current / 10**6, peak / 10**6])

    row.append(time() - start)
    return (filename, f"{filename[:-5]}.parquet")


counts = [500]

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
col_name_sub = ['Time', 'Peak(MB)', 'Current(MB)']
cols = []
for i in col_name:
    for j in col_name_sub:
        cols.append((i, j))
cols.append(('Total',))
col_list = pd.MultiIndex.from_tuples(cols)

df = pd.DataFrame(rows, counts, col_list)
print(df)

df.to_excel("./results.xlsx")
