import os
import pandas as pd
from datetime import datetime

publishing_group = 'publishing_group=VNR'


def create_data_frame(data):
    df = pd.DataFrame(data)
    return df


def get_unix_timestamp(provided_date):
    unix_time_stamp = int(datetime.strptime(str(provided_date), '%Y-%m-%d').
                          strftime("%s"))
    return unix_time_stamp