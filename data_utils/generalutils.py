import os
import pandas as pd
from datetime import datetime

publishing_group = 'publishing_group=VNR'


def create_data_frame(data):
    df = pd.DataFrame(data)
    return df


def get_unix_timestamp(provided_date):
    unix_time_stamp = int(datetime.strptime(str(provided_date), '%Y-%m-%d').
                          strftime('%s'))
    return unix_time_stamp


def get_fb_target_prefix(app_id):
    target_prefix = [publishing_group, f'provider=facebook', f'app_id='
                                        f'{app_id}', f'type=page_impressions']
    return target_prefix


def get_target_path(target):
    '''Creates target key with provided target values.
    Arguments:
        target_path {[list]} -- [list of directories and file]
    Returns:
        [string] -- [target path to the s3 file]
    '''
    target_path = os.path.join('', *target)
    return target_path


def get_flatten_impressions(impressions):
    impressions_flat = []
    data = impressions['data']
    paging = impressions['paging'] if 'paging' in impressions else None
    prev_page = paging['previous'] if paging and 'previous' in paging else ''
    next_page = paging['next'] if paging and 'next' in paging else ''
    for period in data:
        if 'name' in period and 'values' in period and \
                len(period['values']) > 0:
            for value in period['values']:
                value['id'] = period['id']
                value['name'] = period['name']
                value['period'] = period['period']
                value['title'] = period['title']
                value['description'] = period['description']
                value['previous_page'] = prev_page
                value['next_page'] = next_page
                impressions_flat.append(value)
    return impressions_flat

def drop_unconfigured_columns(df, conf):
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