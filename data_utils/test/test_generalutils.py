"""Test Cases for generalutils library
"""
from data_utils import generalutils as gu
import pytest
import pandas as pd
import datetime
import IPython

def test_get_unix_timestamp():
    """Test function for get_unix_timestamp() function in generalutils
    """
    test_date = '2020-01-01'
    unix_time = gu.get_unix_timestamp(test_date)
    
    assert isinstance(unix_time, int)
    assert unix_time == 1577836800
    
def test_create_data_frame():
    """Test function for create_data_frame() function in generalutils
    """
    test_data = {
        'col1': [1, 2, 3],
        'col2': ['abc', 'def', 'ghi'],
        'col3':[1.0, 2.0, 3.0]
    }
    df = gu.create_data_frame(test_data)
    
    assert isinstance(df,pd.DataFrame)
    assert df['col1'].dtypes.name == 'int64'
    assert df['col2'].dtypes.name == 'object'
    assert df['col3'].dtypes.name == 'float64'

def test_get_target_prefix():
    """Test function for get_target_prefix() function in generalutils
    """
    publishing_group= 'VNR'
    provider = 'facebook'
    page_id= '1234567890'
    prefix= gu.get_target_prefix(publishing_group, provider, page_id)
    
    assert isinstance(prefix, list)
    assert publishing_group in prefix
    assert f'provider={provider}' in prefix
    assert f'partition_page_id={page_id}' in prefix

def test_verify_int():
    test_int = gu.verify_int(10)
    assert test_int == True
    test_int = gu.verify_int(10.5)
    assert test_int == True
    test_int = gu.verify_int('10')
    assert test_int == True
    test_int = gu.verify_int('A10')
    assert test_int == False

def test_verify_and_convert_bool():
    test_bool = gu.verify_and_convert_bool(True)
    assert test_bool == True
    test_bool = gu.verify_and_convert_bool('False')
    assert test_bool == False
    test_bool = gu.verify_and_convert_bool(False)
    assert test_bool == False
    test_bool = gu.verify_and_convert_bool("True")
    assert test_bool == True
    test_bool = gu.verify_and_convert_bool("true")
    assert test_bool == True
    test_bool = gu.verify_and_convert_bool("0")
    assert test_bool == False

def test_check_start_stop_date():
    start_str = "2019-11-11"
    stop_str = "2019-11-12"
    start_date, stop_date = gu.check_start_stop_date(start_str,stop_str)
    # assert 'datetime.datetime' in type(start_date)
    assert type(start_date) == datetime.datetime
    assert type(stop_date) == datetime.datetime

def test_create_time_partition():
    """Test function for create_time_partition() function in generalutils
    """
    test_date = datetime.date.today()
    time_partition = gu.create_time_partition(test_date)
    assert time_partition == f'partition_year={test_date.year}/partition_month={test_date.month}/partition_day={test_date.day}'
    
    time_partition = gu.create_time_partition(test_date, month= True)
    assert time_partition == f'partition_year={test_date.year}/partition_month={test_date.month}'