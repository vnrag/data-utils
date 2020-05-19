"""Test Cases for generalutils library
"""
from data_utils import generalutils as gu
import pytest
import pandas as pd

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
    