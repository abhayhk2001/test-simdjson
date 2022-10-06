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


def compare_gz(parquet_gz, filename):
    with open(parquet_gz, 'rb') as f_in, gzip.open(parquet_gz+'.gz', 'wb') as f_out:
        f_out.writelines(f_in)
    json_gz_size = os.path.getsize(f'./data/{filename}.gz')
    parquet_gz_size = os.path.getsize(parquet_gz+'.gz')
    print("Json .gz file size: ", json_gz_size, " bytes")
    print("Parquet .gz file size: ", parquet_gz_size, " bytes")
    os.remove(parquet_gz+'.gz')


def create_df(rows, counts):
    col_name = ['Formatting', 'Loading File',
                'Pivotting Table', 'Converting Table to Parquet']
    col_name_sub = ['Time', 'Increase']
    cols = [('File',)]
    for i in col_name:
        for j in col_name_sub:
            cols.append((i, j))
    cols.extend([('Total', 'Time'), ('Total', 'Increase')])
    col_list = pd.MultiIndex.from_tuples(cols)
    df = pd.DataFrame(rows, counts, col_list)
    df.to_excel(f"./output/results.xlsx")

    print(df)


def make_time_df(rows, counts):
    cols = ['Start', 'Formatting', 'Loading File',
            'Pivotting Table', 'Converting Table to Parquet']
    df = pd.DataFrame(rows, counts, cols)
    df.to_csv(f"./output/results_time.csv")
    pass


def run(findex, file, count, prev):
    # Starting Time Recording
    total_time = 0
    row = [findex]
    time_row = []
    start = time()
    time_row.append(start)

    # Formatting JSON
    filename = format_file(file, count)
    format_time = time() - start
    total_time += format_time
    time_row.append(start + format_time)
    row.extend(calc_increase(prev, format_time, row))

    start = time()
    # Loading JSON file to Python Dataframe (simdjson vs polars)
    parser = simdjson.Parser()
    doc = parser.load(filename, True)
    df = pd.DataFrame(doc)
    load_time = time() - start
    total_time += load_time
    time_row.append(start + load_time)
    row.extend(calc_increase(prev, load_time, row))

    start = time()
    # Using Pivot table to recieve appropriate output
    df = df.pivot_table(index=['timestamp', 'device_uuid'],
                        columns='data_item_name', values='value', aggfunc='first')
    pivot_time = time() - start
    total_time += pivot_time
    time_row.append(start + pivot_time)
    row.extend(calc_increase(prev, pivot_time, row))

    start = time()
    # Converting to Parquet
    df.to_parquet(f"{filename[:-5]}.parquet", engine="pyarrow")
    convert_time = time() - start
    total_time += convert_time
    time_row.append(start + convert_time)
    row.extend(calc_increase(prev, convert_time, row))
    return (filename, f"{filename[:-5]}.parquet", total_time, row, time_row)


def main(file_counts):
    rows, prev, time_rows = [], [], []
    final_parquet = ""
    counts = []
    for (findex, file, count) in file_counts:
        counts.append(count)
        if count == 0:
            print(f"Running for whole file")
        else:
            print(f"Running for count: {count}")

        (fname, parquet_name, total_time, row,
         time_row) = run(findex, file, count, prev)

        row.extend(calc_increase(prev, total_time, row))
        prev = row
        os.remove(fname)
        if (count != counts[-1]):
            os.remove(parquet_name)
        else:
            final_parquet = "./extras" + parquet_name[6:]
            os.replace(parquet_name, "./extras"+parquet_name[6:])
        rows.append(row)
        time_rows.append(time_row)
    compare_gz(final_parquet, file_counts[-1][1])
    create_df(rows, counts)
    make_time_df(time_rows, counts)


fname1 = "connectdata-day=2022-09-19_device=s_96_0.json"
fname2 = "connectdata-day=2022-09-19_device=s_96_2.json"
fname3 = "connectdata-day=2022-09-19_device=s_96_3.json"

# main([("file1","sample_json_test_data_2.json", 124037)])
# main([("file1",fname1, 453132)])
# main([("file1",fname2, 836753)])
# main([("file1",fname3, 1000000)])
# main([("file1", fname1, 453132), ("file2", fname2, 453132), ("file2", fname2, 836753),("file3", fname3, 453132), ("file3", fname3, 836753), ("file3", fname3, 1000000)])
# main([("file3", fname3, 1000000), ("file2", fname2, 836753), ("file1", fname1, 453132),("file3", fname3, 836753), ("file2", fname2, 453132), ("file3", fname3, 453132)])
# main([("file2", fname2, 453132), ("file2", fname2, 500000), ("file2", fname2, 550000), ("file2", fname2, 600000), ("file2", fname2, 700000), ("file2", fname2, 800000), ("file2", fname2, 836753)])
main([("file1", fname1, 100000), ("file1", fname1, 200000), ("file1", fname1, 300000), ("file1", fname1, 400000), ("file1", fname1, 500000),
     ("file1", fname1, 600000), ("file1", fname1, 700000), ("file1", fname1, 800000), ("file1", fname1, 836753), ("file1", fname1, 900000), ("file1", fname1, 1000000)])
