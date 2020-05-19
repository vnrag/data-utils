"""Test Cases for awsutils library

Attributes
----------
config : dict
    Configuration used to create S3_base object
EXPORT_BUCKET : str
    Bucket to export resulting data to
"""
import pytest
from data_utils import awsutils as awsu
import boto3
from moto import mock_s3, mock_ssm
import pandas as pd
import os
import io

config = {'client': True, 'resource': True}
EXPORT_BUCKET="OUTPUT"


@pytest.fixture(scope='function')
def aws_credentials():
    """Mocks AWS Credentials for moto.
    """
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['EXPORT_BUCKET'] = EXPORT_BUCKET

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
        s3 = boto3.resource('s3', region_name='eu-central-1')
        s3.create_bucket(Bucket=EXPORT_BUCKET)
        yield s3

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
        ssm = boto3.client('ssm', region_name='eu-central-1')
        ssm.put_parameter(Name= 'ExternalBucketName', Value=EXPORT_BUCKET, Type='String')
        yield ssm

def test_upload_parquet_to_s3(s3, ssm):
    """Test function for upload_parquet_to_s3() function in awsutils
    
    Parameters
    ----------
    s3 : Mocked S3 Instance
        Description
    ssm : Mocked SSM service
        Description
    """
    s3_base = awsu.S3Base(config)
    
    bucket = s3_base.get_ssm_parameter('ExternalBucketName')
    target_key = 'publishing_group=VNR/provider=facebook/page_id=1234567890/year=2020/month=01/day=01/11111111.parquet'
    
    s3_uri = os.path.join(f's3://{bucket}',target_key)

    d = {'col1': [1, 2], 'col2': ['abc', 'def']}
    parquet_context = pd.DataFrame(d)
    
    s3_base.upload_parquet_to_s3(s3_uri, parquet_context)
    buffer = io.BytesIO()
    object = s3.Object(bucket, target_key)
    object.download_fileobj(buffer)
    df = pd.read_parquet(buffer)
