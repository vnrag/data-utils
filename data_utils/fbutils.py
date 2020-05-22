"""A library containing commonly used utils for using facebbok API

Attributes
----------
config : TYPE
    Loads configuration from config file
"""
import json
from datetime import datetime

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

def get_flatten_impressions(impressions, page_name):
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
                string_date = (value['end_time'].strip().split('T')[0]).\
                replace('-', '/')
                date_val = datetime.strptime(string_date,
                '%Y/%m/%d').strftime("%Y/%m/%d")
                if type(value['value']) == dict:
                    for key, val in value['value'].items():
                        if date_val not in impressions_flat:
                            impressions_flat[date_val] = []

                        temp = {'page_name': page_name, 'name':
                            period['name'], 'period': period['period'],
                                'details': key, 'value': val,
                                'end_time': date_val}
                        impressions_flat[date_val].append(temp)
                else:
                    if date_val not in impressions_flat:
                        impressions_flat[date_val] = []

                    temp = {'page_name': page_name, 'name': period['name'],
                            'period': period['period'], 'details': '',
                            'value': value['value'], 'end_time': date_val}

                    impressions_flat[date_val].append(temp)
    return impressions_flat
    
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

def map_fb_output_data(df):
    """Maps the data data types of facebook results to the respective predefined ones: 
    period, value and end_time to Strings
    
    Parameters
    ----------
    df : Pandas DataFrame
        Description
    
    Returns
    -------
    Pandas DataFrame
        Description
    """
    df['period'] = df['period'].astype(str)
    df['value'] = df['value'].astype(str)
    df['end_time'] = df['end_time'].astype(str)
    return df

def get_pages_info_dict(json_data):
    """Gets the information of facebook pages
    
    Parameters
    ----------
    json_data : json
        Description
    
    Returns
    -------
    list
        Description
    """
    pages_info_dict_list = []
    for page in json_data['data']:
        pages_info_dict_list.append({'id': page['id'], 'access_token':
            page['access_token'], 'name': page['name']})
    return pages_info_dict_list