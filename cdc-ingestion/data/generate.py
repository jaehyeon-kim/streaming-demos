import os
import re
import csv


def read_csv(filename: str, pathname: str):
    dics = []
    with open(os.path.join(pathname, filename)) as f:
        csv_reader = csv.DictReader(f)
        for row in csv_reader:
            dics.append(row)
        return dics


def read_items(src_path: str = os.path.dirname(os.path.realpath(__file__))):
    items = {}
    src_files = os.listdir(os.path.join(src_path, "csvs"))
    for filename in src_files:
        dics = read_csv(filename, os.path.join(src_path, "csvs"))
        items[re.sub(".csv$", "", filename)] = dics
    return items


if __name__ == "__main__":
    items = read_items()
    print(items)
