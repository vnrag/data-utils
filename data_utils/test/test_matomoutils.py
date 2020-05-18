"""Test Cases for matomoutils library
"""
import pytest
from data_utils import matomoutils as mu

def test_create_matomo_url():
    """Test function for create_matomo_url() function in matomoutils
    """
    export_date= '1234567890'
    matomo_api_key= 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    limit= 100
    offset= 10
    url= mu.create_matomo_url(export_date, matomo_api_key, limit, offset)
    
    assert isinstance(url, str)
    assert export_date in url
    assert matomo_api_key in url
    assert limit in url
    assert offset in url