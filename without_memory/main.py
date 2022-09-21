import simdjson
import pandas as pd
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


def calc_increase(prev, time, row):
    if (len(prev) == 0):
        increase = ''
    else:
        increase = ((time - prev[len(row)])/prev[len(row)]) * 100
        increase = str(round(increase, 3)) + '%'

    return ([round(time, 4), increase])


def run(file, count, prev):
    # Starting Time Recording
    total_time = 0
    start = time()
    row = []

    # Formatting JSON
    filename = format_file(file, count)
    format_time = time() - start
    total_time += format_time
    row.extend(calc_increase(prev, format_time, row))

    start = time()
    # Loading JSON file to Python Dataframe
    parser = simdjson.Parser()
    doc = parser.load(filename, True)
    df = pd.DataFrame(doc)
    load_time = time() - start
    total_time += load_time
    row.extend(calc_increase(prev, load_time, row))

    start = time()
    # Using Pivot table to recieve appropriate output
    df = df.pivot_table(index=['timestamp', 'device_uuid'],
                        columns='data_item_name', values='value', aggfunc='first')
    pivot_time = time() - start
    total_time += pivot_time
    row.extend(calc_increase(prev, pivot_time, row))

    start = time()
    # Converting to Parquet
    df.to_parquet(f"{filename[:-5]}.parquet", engine="pyarrow")
    convert_time = time() - start
    total_time += convert_time
    row.extend(calc_increase(prev, convert_time, row))
    return (filename, f"{filename[:-5]}.parquet", total_time, row)


def create_df(fname, rows, counts):
    col_name = ['Formatting', 'Loading File',
                'Pivotting Table', 'Converting Table to Parquet']
    col_name_sub = ['Time', 'Increase']
    cols = []
    for i in col_name:
        for j in col_name_sub:
            cols.append((i, j))
    cols.extend([('Total', 'Time'), ('Total', 'Increase')])
    col_list = pd.MultiIndex.from_tuples(cols)
    df = pd.DataFrame(rows, counts, col_list)
    df.to_excel(f"./output/results_{fname[:-5]}.xlsx")

    print(df)


def main(file, counts):
    rows, prev = [], []
    for count in counts:
        if count == 0:
            print(f"Running for whole file")
        else:
            print(f"Running for count: {count}")

        (fname, parquet_name, total_time, row) = run(
            file, count, prev)

        row.extend(calc_increase(prev, total_time, row))
        prev = row
        os.remove(fname)
        os.remove(parquet_name)
        rows.append(row)
    create_df(file, rows, counts)


main("sample_json_test_data_2.json", [500, 10000, 50000, 100000, 0])