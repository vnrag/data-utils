"""A library containing commonly used utils for general purposes
"""
import os
import pandas as pd
from datetime import datetime

def create_data_frame(data):
    """Converts input data into a pandas DataFrame
    
    Parameters
    ----------
    data : TYPE
        Description
    
    Returns
    -------
    TYPE
        Description
    """
    df = pd.DataFrame(data)
    return df


def get_unix_timestamp(provided_date):
    """Converts a string date to Unix time. Input has to be in the format '%Y-%m-%d'
    
    Parameters
    ----------
    provided_date : String
        Description
    
    Returns
    -------
    int
        representation of date in Unix time (Epoch time, POSIX time)
    """
    unix_time_stamp = int(datetime.strptime(str(provided_date), '%Y-%m-%d').
                          strftime("%s"))
    return unix_time_stamp


def get_target_path(target):
    """Creates target key with provided target values.
    
    Parameters
    ----------
    target : list
        list of directories and file
    
    Returns
    -------
    String
        target path to the s3 file
    """
    target_path = os.path.join('', *target)
    return target_path



def drop_unconfigured_columns(df, conf):
    """Summary
    
    Parameters
    ----------
    df : TYPE
        Description
    conf : TYPE
        Description
    
    Returns
    -------
    TYPE
        Description
    """
    all_used_columns = []
    if 'int_columns' in conf:
        all_used_columns = conf['int_columns']
    if 'str_columns' in conf:
        all_used_columns.extend(conf['str_columns'])
    if 'float_columns' in conf:
        all_used_columns.extend(conf['float_columns'])
    if 'datetime_columns' in conf:
        all_used_columns.extend(conf['datetime_columns'])
    df = df[df.columns.intersection(all_used_columns)]
    return df