import pandas as pd


def make_df(rows, counts, fname):
    cols = ['Formatting', 'Loading File',
            'Pivotting Table', 'Converting Table to Parquet']
    df = pd.DataFrame(rows, counts, cols)
    df.to_csv(f"./output/{fname}.csv")


def parse_results(resource_file):
    timestamps = pd.read_csv("./output/results_time.csv")
    counts = timestamps.iloc[:, 0]
    timestamps.drop(timestamps.columns[0], axis=1, inplace=True)
    collectd_results = pd.read_csv(f"./extras/collectd_{resource_file}.csv")
    rows = []
    for i in range(timestamps.shape[0]):
        intervals = []
        k = 0
        for j in range(timestamps.shape[1]):
            timestamp = timestamps.iloc[i, j]
            while (k < collectd_results.shape[0] and collectd_results.iloc[k, 0] <= timestamp):
                k += 1
            intervals.append(k-1)
        k = 0
        row = []
        while (k < (len(intervals)-1)):
            row.append(
                round(collectd_results.iloc[intervals[k]:intervals[k+1], 1].mean()/(10**9), 4))
            k += 1
        rows.append(row)
    make_df(rows, counts, f"./output/results_{resource_file}.csv")


parse_results("memory")
