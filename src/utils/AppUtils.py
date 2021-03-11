import numpy as np


def get_np_array(arr_len, value, data_type):
    arr = np.empty(arr_len, dtype=data_type)
    arr.fill(value)
    return arr
