"""A library containing commonly used utils for using facebbok API

Attributes
----------
config : TYPE
    Loads configuration from config file
"""
import json
import urllib3

from .config import load_config
config = load_config()

def create_fb_url_to_get_accounts(user_id, user_token):
    """Creates a URL that can be used to call facebook GRAPH API with a user id and user token
    to get list of pages, page ids and page tokens
    
    Parameters
    ----------
    app_id : String
        Description
    access_token : String
        Description
    
    Returns
    -------
    String
        URL which can be used to call get request from facebook GRAPH API
    """
    url = f'{config["fb_data_url"]}{user_id}/accounts?' \
          f'access_token={user_token}&limit=100'
    return url


def create_fb_url_data(start, stop, page_id, page_token):
    """Creates a URL that can be used to call facebook GRAPH API with a page id and page token
    to get the desired metrics for the page
    
    Parameters
    ----------
    start : int
        start date in seconds
    stop : int
        stop date in seconds
    page_id : String
        Description
    page_token : String
        Description
    
    Returns
    -------
    String
        URL which can be used to call get request from facebook GRAPH API
    """
    url = f'{config["fb_data_url"]}{page_id}/insights?pretty' \
          f'=0&{config["fb_metric"]}&since={start}&until={stop}&' \
          f'access_token={page_token}'
    return url


def get_flatten_impressions(impressions):
    """Flattens the result of impressions data requested by facebook GRAPH API
    
    Parameters
    ----------
    impressions : json
        Description
    
    Returns
    -------
    dict
        Flattened dictionary containing impressions data
    """
    impressions_flat = {}
    data = impressions['data']
    for period in data:
        if 'name' in period and 'values' in period and \
                len(period['values']) > 0:
            for value in period['values']:
                if type(value['value']) == dict:
                    for key, val in value['value'].items():
                        if period['name'] not in impressions_flat:
                            impressions_flat[period['name']] = []

                        temp = {'id': period['id'], 'name': period['name'],
                            'period': period['period'], 'value':
                                f'{key}: {val}', 'end_time':
                                value['end_time']}
                        impressions_flat[period['name']].append(temp)
                else:
                    if period['name'] not in impressions_flat:
                        impressions_flat[period['name']] = []

                    temp = {'id': period['id'], 'name': period['name'],
                            'period': period['period'], 'value':
                                value['value'], 'end_time':
                                value['end_time']}
                    impressions_flat[period['name']].append(temp)
    return impressions_flat

def get_fb_target_prefix(root_folder, page_id):
    """Creates a list of prefixes hierarchy for facebook api results
    
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
    target_prefix = [root_folder, f'provider=facebook', f'page_id='
                                        f'{page_id}']
    return target_prefix
    
def get_users_id_token_dict(json_data):
    """Summary
    
    Parameters
    ----------
    json_data : json
        Description
    
    Returns
    -------
    TYPE
        Description
    """
    user_dict_list = []
    for user in json_data['data']:
        user_dict_list.append({'id': user['id'], 'access_token':
            user['access_token']})
    return user_dict_list