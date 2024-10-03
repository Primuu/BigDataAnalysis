from itertools import repeat
import pandas as pd
from datetime import datetime
from filesplit.split import Split
from multiprocessing import Pool
import os


def count_time(func):
    def wrapper(*args, **kwargs):
        start = datetime.now()
        func(*args, **kwargs)
        print(f"Reading time {func.__name__}: {datetime.now() - start} seconds")
        return func(*args, **kwargs)
    return wrapper


def apply_args_and_kwargs(func, args, kwargs):
    return func(*args, **kwargs)


def starmap_with_kwargs(pool, func, args_iter, kwargs_iter):
    args_for_starmap = zip(repeat(func), args_iter, kwargs_iter)
    return pool.starmap(apply_args_and_kwargs, args_for_starmap)


def split_file(filepath, chunksize, destination):
    split = Split(filepath, destination)
    split.bylinecount(linecount=chunksize, includeheader=True)


@count_time
def load_files(directory, processes):

    files = [[f"{directory}/{f}"] for f in os.listdir(directory) if f.endswith(".csv")]

    kwargs_list = [
        {
            'on_bad_lines': "skip",
        }
        for n in range(len(files))
    ]

    pool = Pool(processes=processes)
    args_iter = files

    results = starmap_with_kwargs(pool, pd.read_csv, args_iter, kwargs_list)
    results = pd.concat(results)

    return results


if __name__ == '__main__':
    split_file('instagram.csv', 800_000, 'data')
    # 4 cores
    #  case 1: 4-2=2
    #  case 2: (4-2)*2=4
    df_multi_1 = load_files('data', 2)
    df_multi_2 = load_files('data', 4)
    df_multi_1.info()
    df_multi_2.info()
