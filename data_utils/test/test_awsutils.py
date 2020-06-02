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
import IPython
import pyarrow.parquet as pq
import s3fs
import json
import csv

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

def test_create_bucket(s3):
    """Test function for create_bucket() function in awsutils
    
    Parameters
    ----------
    s3 : Mocked S3 Instance
        Description
    """
    s3_base = awsu.S3Base()
    
    bucket = 'testbucket'
    s3_base.create_bucket(bucket)
    assert bucket in s3_base.list_buckets()

def test_create_s3_uri(s3):
    """Test function for create_s3_uri() function in awsutils
    
    Parameters
    ----------
    s3 : Mocked S3 Instance
        Description
    """
    s3_base = awsu.S3Base()
    
    bucket = 'testbucket'
    key = 'test_key'
    file = 'test_file'
    file_extension = 'csv'
    
    s3_uri_with_extension = s3_base.create_s3_uri(bucket, key, file, file_extension)
    assert s3_uri_with_extension == 's3://testbucket/test_key/test_file.csv'
    
    s3_uri_without_extension = s3_base.create_s3_uri(bucket, key, file)
    assert s3_uri_without_extension == 's3://testbucket/test_key/test_file'

def test_check_if_object_exists(s3, ssm):
    """Test function for check_if_object_exists() function in awsutils
    
    Parameters
    ----------
    s3 : Mocked S3 Instance
        Description
    ssm : Mocked SSM service
        Description
    """
    s3_base = awsu.S3Base()
    ssm_base = awsu.SSMBase()
    
    content = 'abcdef'
    bucket = ssm_base.get_ssm_parameter('ExternalBucketName')
    true_key = 'publishing_group=VNR/year=2020/month=01/day=01/11111111.csv'
    fake_key = 'abc/def/ghi.csv'
    
    s3_base.upload_object_to_s3(content, bucket, true_key)
    
    exists = s3_base.check_if_object_exists(bucket, true_key)
    doesnt_exist = s3_base.check_if_object_exists(bucket, fake_key)
    
    assert exists is True
    assert doesnt_exist is False

def test_upload_parquet_to_s3(s3, ssm):
    """Test function for upload_parquet_to_s3() function in awsutils
    
    Parameters
    ----------
    s3 : Mocked S3 Instance
        Description
    ssm : Mocked SSM service
        Description
    """
    fs = s3fs.S3FileSystem()
    s3_base = awsu.S3Base()
    ssm_base = awsu.SSMBase()
    
    bucket = ssm_base.get_ssm_parameter('ExternalBucketName')
    target_key = 'publishing_group=VNR/year=2020/month=01/day=01/11111111.parquet'
    s3_uri = os.path.join(f's3://{bucket}',target_key)
    
    d = {'col1': [1, 2], 'col2': ['abc', 'def']}
    parquet_context = pd.DataFrame(d)

    s3_base.upload_parquet_to_s3(s3_uri, parquet_context)
    
    dataset = pq.ParquetDataset(s3_uri, filesystem=fs)
    table = dataset.read()
    df = table.to_pandas() 
    
    assert 'col1' in df.columns
    assert 'col2' in df.columns
    assert df['col1'][0] == 1
    assert df['col2'][1] == 'def'

def test_upload_as_json_to_s3(s3, ssm):
    """Test function for upload_as_json_to_s3() function in awsutils
     
    Parameters
    ----------
    s3 : Mocked S3 Instance
        Description
    ssm : Mocked SSM service
        Description
    """
    s3_base = awsu.S3Base()
    ssm_base = awsu.SSMBase()
    
    bucket = ssm_base.get_ssm_parameter('ExternalBucketName')
    target_key = 'publishing_group=VNR/year=2020/month=01/day=01/11111111.json'

    
    with open("test/res/test_data.json") as f:
        content = f.read()
    content = json.loads(content)
    
    s3_base.upload_as_json_to_s3(content, bucket, target_key)
    
    body = s3.Object(bucket, target_key).get()['Body'].read().decode("utf-8")
    json_data = json.loads(body)

    assert len(json_data) == 3
    assert json_data["1"]["col2"] == "abc"
    assert json_data["2"]["col1"] == 2
    assert json_data["3"]["col2"] == "mno"

def test_upload_as_csv_to_s3(s3, ssm):
    """Test function for upload_as_csv_to_s3() function in awsutils
     
    Parameters
    ----------
    s3 : Mocked S3 Instance
        Description
    ssm : Mocked SSM service
        Description
    """
    s3_base = awsu.S3Base()
    ssm_base = awsu.SSMBase()
    
    bucket = ssm_base.get_ssm_parameter('ExternalBucketName')
    target_key = 'publishing_group=VNR/year=2020/month=01/day=01/11111111.csv'
    
    with open("test/res/test_data.csv") as f:
        content = f.read()
    
    s3_base.upload_as_csv_to_s3(content, bucket, target_key)
    
    body = s3.Object(bucket, target_key).get()['Body'].read().decode("utf-8")
    
    lines = body.splitlines()
    reader = csv.reader(lines, delimiter=',')
    data_list = list(reader)
    
    assert data_list[1][1] == "abc"
    assert data_list[2][0] == "2"
    assert data_list[3][1] == "ghi"
