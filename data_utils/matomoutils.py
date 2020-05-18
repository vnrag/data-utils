"""A library containing commonly used utils for motomo API

Attributes
----------
config : TYPE
    Loads configuration from config file
"""
from .config import load_config
config = load_config()

def create_matomo_url(export_date, matomo_api_key, limit, offset):
    """Summary
    
    Parameters
    ----------
    export_date : TYPE
        Description
    matomo_api_key : TYPE
        Description
    limit : TYPE
        Description
    offset : TYPE
        Description
    
    Returns
    -------
    TYPE
        Description
    """
    url = f'{config["matomo_url"]}{export_date}&format=json&token_auth=' \
          f'{matomo_api_key}&filter_limit={limit}&filter_offset={offset}'
    return url