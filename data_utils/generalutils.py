"""A library containing commonly used utils for general purposes
"""
import os
import pandas as pd
from datetime import datetime
import hashlib
import sys
import json
from datetime import datetime
import datetime as dt

if sys.version_info[0] >= 3:
    unicode = str

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
    return target_path + file_extension if file_extension else target_path



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

def check_start_stop_date(start_date, stop_date):
    if start_date:
        try:
            start_date = datetime.strptime(start_date, '%Y-%m-%d')
        except Exception as e:
            print ("Parsing Error for Date:%s" % e)
            start_date = dt.datetime.now() - dt.timedelta(days=1)
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
    else:
        start_date = dt.datetime.now() - dt.timedelta(days=1)
        # there is no stop date so we return only one day
        return start_date, start_date