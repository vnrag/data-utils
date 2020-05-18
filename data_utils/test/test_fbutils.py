"""Test Cases for fbutils library
"""
import pytest
import json
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
    flat_impressions = fu.get_flatten_impressions(impressions)
    
    assert isinstance(flat_impressions, dict)
    assert 'page_fans_by_like_source' in flat_impressions
    assert 'page_views_total' in flat_impressions
    assert 'page_fans_gender_age' in flat_impressions
    assert 'page_impressions_unique' in flat_impressions
    assert 'page_engaged_users' in flat_impressions
    assert flat_impressions['page_engaged_users'][0]['value'] == 1875
    assert flat_impressions['page_engaged_users'][1]['end_time'] == '2019-12-14T08:00:00+0000'

def test_get_fb_target_prefix():
    """Test function for get_fb_target_prefix() function in fbutils
    """
    root_folder = 'VNR'
    page_id= '1234567890'
    prefix= fu.get_fb_target_prefix(root_folder, page_id)
    
    assert isinstance(prefix, list)
    assert root_folder in prefix
    assert f'page_id={page_id}' in prefix

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