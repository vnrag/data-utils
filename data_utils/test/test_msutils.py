"""Test Cases for awsutils library

Attributes
----------
EXPORT_BUCKET : str
    Bucket where the resulting data will be written
"""
import pytest
from data_utils import awsutils as awsu
import boto3
from moto import mock_s3, mock_ssm
import pandas as pd
import os
import IPython
import pyarrow.parquet as pq
import s3fs
import json
import csv

EXTERNAL_BUCKET= 'OUTPUT'

@pytest.fixture(scope='function')
def aws_credentials():
    """Mocks AWS Credentials for moto.
    """
    os.environ['EXTERNAL_BUCKET'] = EXTERNAL_BUCKET

@pytest.fixture(scope='function')
def ssm(aws_credentials):
     """Mocks a SSM service for moto with a parameter holding value for testing bucket
     
     Parameters
     ----------
     aws_credentials : TYPE
         Description
     
     Yields
     ------
     TYPE
         Description
     """
     with mock_ssm():
        ssm = boto3.client('ssm')
        ssm.put_parameter(Name= 'ExternalBucketName', Value= os.environ.get('EXTERNAL_BUCKET'), Type= 'String')
        yield ssm

def test_get_ssm_parameter(ssm):
    """Test function for get_ssm_parameter() function in awsutils
    
    Parameters
    ----------
    ssm : Mocked SSM service
        Description
    """
    ssm_base = awsu.SSMBase()
    bucket = ssm_base.get_ssm_parameter('ExternalBucketName')
    
    assert bucket == 'OUTPUT'


@pytest.fixture(scope='function')
def s3(aws_credentials):
    """Mocks a S3 instace for moto with a testing bucket
    
    Parameters
    ----------
    aws_credentials : TYPE
        Description
    
    Yields
    ------
    TYPE
        Description
    """
    with mock_s3():
        s3 = boto3.resource('s3')
        s3.create_bucket(Bucket= os.environ.get('CONFIG_BUCKET'))
        s3.create_bucket(Bucket= os.environ.get('EXPORT_BUCKET'))
        s3.create_bucket(Bucket= os.environ.get('LOG_BUCKET'))        
        yield s3

def test_fetch_onedrive_data(s3):
    """Test function for fetch_onedrive_data() function in msutils
    
    Parameters
    ----------
    s3 : Mocked S3 Instance
        Description
    """
    # use test_data.csv as input from mocked api request of onedrive
    s3_base = awsu.S3Base()
    assert False