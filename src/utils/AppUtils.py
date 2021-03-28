from datetime import timedelta
import numpy as np


def get_np_array(arr_len, value, data_type):
    arr = np.empty( arr_len, dtype=data_type )
    arr.fill( value )
    return arr


def get_prev_date_str(today_dt, no_of_days_back=1, format='%d-%b-%Y'):
    prev_dt = today_dt - timedelta( days=no_of_days_back )
    return prev_dt.strftime( format )


def get_next_date_str(today_dt, no_of_days_nxt=1, format='%d-%b-%Y'):
    prev_dt = today_dt + timedelta( days=no_of_days_nxt )
    return prev_dt.strftime( format )


def get_date_str(dt, format='%d-%b-%Y'):
    return dt.strftime( format )


def get_prev_dates_str(today, day_limit):
    date_list = []
    for i in range( day_limit ):
        dt_str = get_prev_date_str( today_dt=today, no_of_days_back=i )
        date_list.append( dt_str )
    dates = tuple( date_list )
    return dates
