"""A library containing commonly used utils for general purposes
"""
import os
import logging
import pandas as pd
from datetime import datetime
import hashlib
import sys
import json
import datetime as dt
from pytz import timezone

if sys.version_info[0] >= 3:
    unicode = str

from .config import load_config


def get_logger(name):
    """Gets logger

    Returns:
        [instance] -- [Instance for Logger]
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logging.info(name)
    if not logger.hasHandlers():
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        # create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        # add formatter to ch
        ch.setFormatter(formatter)
        # add ch to logger
        logger.addHandler(ch)
    return logger


def get_config():
    """Loads config from config folder

    Returns:
        [json] -- [Config from config.json]
    """
    return load_config()


def create_data_frame(data=None, columns= None):
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
    df = pd.DataFrame(data=data, columns=columns)
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


def flatten_list_col(df, index_cols):
    """Flattens dataframe based on the column with list value

    Parameters
    ----------
    df : Dataframe
    index_cols: Column we want to keep as index

    Returns
    -------
    TYPE: Pandas-DF
        Description
    """

    df = (df.set_index(index_cols).apply(
        lambda x: x.str.split(',').explode()
    ).reset_index())

    new_df = df.convert_dtypes()
    return new_df


def explode_col_in_df(df, col):
    """
    Explodes the given column of the dataframe and recreates the column
    with new values
    """
    df = df.explode(col)
    col_values = list(df[col].values)
    new_values = []
    for val in col_values:
        new_val = val if str(val) == 'nan' else str(val)
        new_val = new_val if new_val else None
        new_values.append(new_val)
    df[col] = new_values
    return df


def flatten_list(val):
    val = ','.join(list(map(lambda x: str(x), val)))
    return val


def flatten_bytes_list(val):
    val = ','.join(list(map(lambda x: str(x.decode('utf-8')), val)))
    return val


def check_if_is_pandas_df(df_val):
    """

    Parameters
    ----------
    df_val: python variable

    Returns bool
    -------

    """
    is_df = False

    if isinstance(df_val, pd.DataFrame):
        is_df = True

    return is_df


def convert_string_col_to_int(df, col):
    converted_df = pd.to_numeric(df[col]).astype('Int64')
    return converted_df

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


def get_target_path(target, file_extension= None):
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
    return target_path + f'.{file_extension}' if file_extension else target_path


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


def calc_time_diff(stop_date, start_date):
    diff = (stop_date - start_date) 
    return diff.days


def verify_string(val):
    if (type(val) == unicode or type(val) == str):
        return True
    return False


def verify_not_empty_string(val):
    return bool(val.strip())


def verify_string_len(max_len, val):
    if (type(val) == unicode or type(val) == str) and len(val) <= max_len:
        return True
    return False


def verify_int(val):
    try:
        int(val)
    except:
        return False
    return True


def verify_and_convert_bool(val):
    if val in ['true', '1', 'J', 'y', 'yes', 'Ja', 'True', 'Y']:
        return True
    elif val == True:
        return True
    return False


def verify_and_convert_string(raw_value, key, element):
    try:
        if verify_not_empty_string(raw_value) and verify_string(raw_value) and not is_invalid_string(raw_value):
            element[key] = raw_value
    except:
        print ("for key: %s value: %s not convertable to string" % (key,raw_value) )


def verify_and_convert_int(raw_value, key, element):
    try:
        element[key] = int(raw_value)
    except:
        print ("for key: %s value: %s not convertable to int" % (key,raw_value) )


def verify_and_convert_float(raw_value, key, element):
    try:
        element[key] = float(raw_value)
    except:
        print ("for key: %s value: %s not convertable to float" % (key,raw_value) )


def verify_string_or_int(val):
    if (type(val) == unicode or type(val) == str):
        return True
    try:
        int(val)
        return True
    except:
        pass
    return False


def is_invalid_string(value):
    '''
    Check if there are whitespace in the value
    returns True or False. True means that there are whitspaces.
    '''
    return not value or value.isspace()


def check_required_fields(params, fields):
    for name, f in fields.items():
        if name not in params:
            return None
        if not f(params[name]):
            return None


def parse_file_to_json(raw_events):
    if raw_events:
        return [json.loads(i) for i in raw_events]
    else:
        return None


def check_start_stop_date(start_date=None, stop_date=None, today=False, shift_in_days=None, to_timezone='UTC'):
    """returns a pair of timestamps as start- and stop-date

    Parameters
    ----------
    start_date : string %Y-m-d
    stop_date : string %Y-m-d
    today : boolean
    shift_in_days : integer
    timezone : string

    Returns
    -------
    array
    """
    # the behaviour of the shift-parameter is different if today is true or false.
    # if today is true, the function will return only an interval of one day based on the current date.
    # if today is false, the function will return an interval based on yesterday.
    tz = timezone(to_timezone)
    if today:
        if shift_in_days:
            try:
                days = int(shift_in_days)
            except Exception as e:
                print ("shift_in_days is not an integer: %s" % e)
                days = 0
        else:
            days = 0
    else:
        if shift_in_days:
            try:
                days = int(shift_in_days) + 1
            except Exception as e:
                print ("shift_in_days is not an integer: %s" % e)
                days = 1
        else:
            days = 1

    if start_date:
        try:
            start_date = datetime.strptime(start_date, '%Y-%m-%d')
        except Exception as e:
            print ("Parsing Error for Date:%s" % e)
            start_date = dt.datetime.now(tz) - dt.timedelta(days=days)
        if stop_date:
            try:
                stop_date = datetime.strptime(stop_date, '%Y-%m-%d')
                return start_date, stop_date
            except Exception as e:
                print ("Parsing Error for Date:%s" % e)
                # there is no stop date so we return only one day
                return start_date, start_date
        else:
            return start_date, start_date
    elif today:
        start_date = dt.datetime.now(tz) - dt.timedelta(days=days)
        return start_date, start_date
    else:
        start_date = dt.datetime.now(tz) - dt.timedelta(days=days)
        stop_date = dt.datetime.now(tz) - dt.timedelta(days=1)
        # there is no start date so we return only one day
        # if there is a shift in days we extract all days from start-date minus shift-in-days until yesterday
        return start_date, stop_date


def create_time_partition(partition_date, month=False):
	'''
	Formats partition_date for use as path in buckets.

	return -- formatted partition_date for uns in file path
	'''
	# needs datetime !
	if month:
		return os.path.join(f'partition_year={str(partition_date.year)}',f'partition_month={str(partition_date.month)}' )
		#return partition_date.strftime("year=%Y/month=%m")
	else:
		return os.path.join(f'partition_year={str(partition_date.year)}',f'partition_month={str(partition_date.month)}',f'partition_day={str(partition_date.day)}')
		#return partition_date.strftime("year=%Y/month=%m/day=%d")
