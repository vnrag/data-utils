"""Test Cases for apiutils library
"""
import pytest
from data_utils import apiutils as au

def test_handle_get_request():
    """Test function for handle_get_request() function in apiutils
    """
    url = 'https://reqbin.com/echo/get/json'
    result = au.handle_get_request(url)
    
    assert isinstance(result, dict)
    assert 'success' in result
    assert result['success'] == 'true'