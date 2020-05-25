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


def create_locally(publishing_group, provider, app_id, df, details):
    """[summary]

    Arguments:
        publishing_group {[string]} -- [Name for publishing group]
        provider {[string]} -- [Provider name]
        app_id {[string]} -- [Id for app]
        df {[string]} -- [Dataframe to be inserted]
        details {[string]} -- [Details for the target name]

    Returns:
        [type] -- [description]
    """
    target_prefix = get_target_prefix(publishing_group, provider, app_id)
    target_prefix.extend(details)

    file_path = get_target_path(target_prefix)
    dir = os.path.dirname(file_path)
    if not os.path.exists(dir):
        os.makedirs(dir)
    with open(file_path, 'w') as f:
        resp = df.to_csv(f)

    output_message = {'Response': resp}
    return output_message
