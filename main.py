import simdjson
import pandas as pd
from time import time
import gzip
import os
import tracemalloc


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

def new_algo(df):
    print(len(df['data_item_name'].unique()))
    pd.DataFrame()
    pass

def run(count):
    start = time()
    filename = format_file("sample_json_test_data_2.json", count)
    format_time = time() - start
    if(len(prev) == 0):
        format_increase = ''
    else:
        format_increase = ((format_time - prev[len(row)])/prev[len(row)]) * 100
        format_increase = str(round(format_increase,3)) + '%'

    current, peak = tracemalloc.get_traced_memory()
    row.extend([round(format_time,4),format_increase, current / 10**6, peak / 10**6])

    parser = simdjson.Parser()
    doc = parser.load(filename, True)
    df = pd.DataFrame(doc)
    load_time = time() - format_time - start
    if(len(prev) == 0):
        load_increase = ''
    else:
        load_increase = ((load_time - prev[len(row)])/prev[len(row)]) * 100
        load_increase = str(round(load_increase,3)) + '%'

    current, peak = tracemalloc.get_traced_memory()
    row.extend([round(load_time,4),load_increase, current / 10**6, peak / 10**6])

    #new_algo(df)
    df = df.pivot_table(index=['timestamp', 'device_uuid'],
                        columns='data_item_name', values='value', aggfunc='first')
    pivot_time = time() - load_time - start
    if(len(prev) == 0):
        pivot_increase = ''
    else:
        pivot_increase = ((pivot_time - prev[len(row)])/prev[len(row)]) * 100
        pivot_increase = str(round(pivot_increase,3)) + '%'
    current, peak = tracemalloc.get_traced_memory()
    row.extend([round(pivot_time,4),pivot_increase, current / 10**6, peak / 10**6])

    df.to_parquet(f"{filename[:-5]}.parquet", engine="pyarrow")
    convert_time = time() - pivot_time - start
    if(len(prev) == 0):
        convert_increase = ''
    else:
        convert_increase = ((convert_time - prev[len(row)])/prev[len(row)]) * 100
        convert_increase = str(round(convert_increase,3)) + '%'
    
    current, peak = tracemalloc.get_traced_memory()
    row.extend([round(convert_time,4),convert_increase, current / 10**6, peak / 10**6])

    total_time = time() - start
    if(len(prev) == 0):
        total_increase = ''
    else:
        total_increase = ((total_time - prev[len(row)])/prev[len(row)]) * 100
        total_increase = str(round(total_increase,3)) + '%'

    row.extend([round(total_time,4),total_increase])
    return (filename, f"{filename[:-5]}.parquet")


counts = [500, 1000, 5000, 10000, 50000, 100000, 124703]

rows = []
prev = []
for count in counts:
    row = []
    print(f"Running for count: {count}")
    tracemalloc.start()
    (fname, parquet_name) = run(count)
    tracemalloc.stop()
    prev = row
    print("\n\n")
    os.remove(fname)
    # os.remove(parquet_name)
    rows.append(row)

col_name = ['Formatting', 'Loading File',
            'Pivotting Table', 'Converting Table to Parquet']
col_name_sub = ['Time', 'Increase', 'Peak(MB)', 'Current(MB)']
cols = []
for i in col_name:
    for j in col_name_sub:
        cols.append((i, j))
cols.extend([('Total','Time'),('Total','Increase')])
col_list = pd.MultiIndex.from_tuples(cols)

df = pd.DataFrame(rows, counts, col_list)
print(df)

df.to_excel("./results_new.xlsx")
