import gzip


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
    return f"./data/{filename[:-5] + '_modified.json'}"


if __name__ == "__main__":
    format_file("sample_json_test_data_2.json", 1000)
