"""A library containing commonly used utils for general purposes
"""
import os
import logging
import pandas as pd
from datetime import datetime

from .config import load_config


def get_logger(name):
    """Gets logger

    Returns:
        [instance] -- [Instance for Logger]
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logging.info(name)
    return logger


def get_config():
    """Loads config from config folder

    Returns:
        [json] -- [Config from config.json]
    """
    return load_config()


def create_data_frame(data=None):
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


def concat_data_frame(list_of_df):
    """Converts input data into a pandas DataFrame

    Parameters
    ----------
    list_of_df : list
        Description

    Returns
    -------
    TYPE
        Description
    """
    df = pd.concat(list_of_df)
    return df


def convert_object_type_to_np_array(df, list_of_cols):
    """Converts object type of df to np array

    Parameters
    ----------
    df : Dataframe
        Description
    list_of_cols: List
        List of column names that needs np array adjustments

    Returns
    -------
    TYPE
        df
    """
    object_type_columns = [col for col in df.columns if df[col].dtype ==
                          'object']
    cols_to_be_adjusted = list(set(list_of_cols).intersection(
        object_type_columns))
    if cols_to_be_adjusted:
        print(f"---- Columns to be adjusted: {cols_to_be_adjusted}")
        for col in cols_to_be_adjusted:
            df[col] = df[col].apply(pd.Series)

    return df


def normalize_json(data):
    """Converts input data into a pandas DataFrame

        Parameters
        ----------
        data : json
            Description

        Returns
        -------
        TYPE
            Description
        """
    df = pd.json_normalize(data)
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


def get_target_prefix(publishing_group, provider, app_id):
    """Creates a list of prefixes hierarchy for the provided api results

    Parameters
    ----------
    root_folder : String
        Description
    page_id : String
        Description

    Returns
    -------
    list
        Description
    """
    target_prefix = [publishing_group, f'provider={provider}',
    f'partition_page_id={app_id}']
    return target_prefix


def create_locally(key_val_dict):
    """[summary]

    Arguments:
        key_val_dict {[dict]} -- [Key and data for a file as key value pair.]
    Returns:
        [type] -- [description]
    """
    for file_path, df in key_val_dict.items():
        file_path = file_path.replace('parquet', 'csv')
        df = pd.DataFrame.from_dict(df)
        dir = os.path.dirname(file_path)
        if not os.path.exists(dir):
            os.makedirs(dir)
        with open(file_path, 'w') as f:
            df.to_csv(f)
