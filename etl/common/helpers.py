from datetime import datetime

from pandas import concat


def date_str_to_int(date_str):
    return int(datetime.strptime(date_str, '%Y-%m-%d').timestamp())


def get_every_nth_df_row(df, n, ignore_index=True):
    return concat([df.iloc[::n, :], df.iloc[-1, :]], ignore_index=ignore_index)


def get_every_nth_index(arr, n):
    return set([i for i in range(len(arr)) if i % n == 0] + [len(arr) - 1])