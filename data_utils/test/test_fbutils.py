"""Test Cases for fbutils library
"""
import pytest
import json
import pandas as pd
from data_utils import fbutils as fu

def test_create_fb_url_to_get_accounts():
    """Test function for create_fb_url_to_get_accounts() function in fbutils
    """
    user_id= '1234567890'
    user_token= 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    url= fu.create_fb_url_to_get_accounts(user_id, user_token)
    
    assert isinstance(url, str)
    assert user_id in url
    assert user_token in url

def test_create_fb_url_data():
    """Test function for create_fb_url_data() function in fbutils
    """
    start= '0000000000'
    stop= '9999999999'
    page_id= '1234567890'
    page_token= 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    url= fu.create_fb_url_data(start, stop, page_id, page_token)
    
    assert isinstance(url, str)
    assert start in url
    assert stop in url
    assert page_id in url
    assert page_token in url

def test_get_flatten_impressions():
    """Test function for get_flatten_impressions() function in fbutils
    """
    with open('test/res/test_impressions.json') as f:
        impressions= json.load(f)
    page_name = 'testing_page'
    flat_impressions = fu.get_flatten_impressions(impressions, page_name)
    
    assert isinstance(flat_impressions, dict)
    impressions_set = set([i['name'] for i in flat_impressions['2019/12/13']])

    assert 'page_fans_by_like_source' in impressions_set
    assert 'page_views_total' in impressions_set
    assert 'page_fans_gender_age' in impressions_set
    assert 'page_impressions_unique' in impressions_set
    assert 'page_engaged_users' in impressions_set
    assert flat_impressions['2019/12/13'][23]['value'] == 1875
    assert flat_impressions['2019/12/14'][0]['end_time'] == '2019/12/14'

def test_get_users_id_token_dict():
    """Test function for get_users_id_token_dict() function in fbutils
    """
    with open('test/res/pages_list.json') as f:
        pages_list = json.load(f)
    pages_info = fu.get_users_id_token_dict(pages_list)
    
    assert isinstance(pages_info, list)
    assert 'id' in pages_info[0]
    assert 'access_token' in pages_info[0]
    assert pages_info[1]['id'] == '0987654321'
    assert pages_info[2]['access_token'] == 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'

def test_map_fb_output_data():
    """Test function for map_fb_output_data() function in fbutils
    """
    test_data = {
        'period': [1, 2, 3],
        'value': ['abc', 'def', 'ghi'],
        'end_time':[1.0, 2.0, 3.0]
    }
    test_df = pd.DataFrame(test_data)
    result_df = fu.map_fb_output_data(test_df)
    
    assert result_df['period'].dtypes.name == 'object'
    assert result_df['value'].dtypes.name == 'object'
    assert result_df['end_time'].dtypes.name == 'object'

def test_get_pages_info_dict():
    """Test function for get_pages_info_dict() function in fbutils
    """
    with open('test/res/pages_list.json') as f:
        pages_list = json.load(f)
    result = fu.get_pages_info_dict(pages_list)
    
    assert isinstance(result, list)
    assert len(result) == 3
    for page in result:
        assert 'id' in page.keys()
        assert 'access_token' in page.keys()
        assert 'name' in page.keys()
    assert result[0]['id'] == '1234567890'
    assert result[0]['access_token'] == 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    assert result[0]['name'] == 'Peter Pan Business'