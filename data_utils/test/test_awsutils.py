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

CONFIG_BUCKET= 'CONF'
INPUT_BUCKET= 'INPUT'
EXPORT_BUCKET= 'OUTPUT'
LOG_BUCKET =  'LOG'

@pytest.fixture(scope='function')
def aws_credentials():
    """Mocks AWS Credentials for moto.
    """
    os.environ['CONFIG_BUCKET'] = CONFIG_BUCKET
    os.environ['INPUT_BUCKET'] = INPUT_BUCKET
    os.environ['EXPORT_BUCKET'] = EXPORT_BUCKET
    os.environ['LOG_BUCKET'] = LOG_BUCKET

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
        ssm.put_parameter(Name= 'ConfigBucketName', Value= os.environ.get('CONFIG_BUCKET'), Type= 'String')
        ssm.put_parameter(Name= 'InputBucketName', Value= os.environ.get('INPUT_BUCKET'), Type= 'String')
        ssm.put_parameter(Name= 'ExternalBucketName', Value= os.environ.get('EXPORT_BUCKET'), Type= 'String')
        ssm.put_parameter(Name= 'LogBucketName', Value= os.environ.get('LOG_BUCKET'), Type= 'String')
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
        s3.create_bucket(Bucket= os.environ.get('INPUT_BUCKET'))
        s3.create_bucket(Bucket= os.environ.get('EXPORT_BUCKET'))
        s3.create_bucket(Bucket= os.environ.get('LOG_BUCKET'))
        
        some_binary_data = b'Here we have some data\nand this is a new line'
        more_binary_data = b'Here we have some more data'
        
        object = s3.Object(os.environ.get('INPUT_BUCKET'), 'TestKey/somefile.txt')
        object.put(Body=some_binary_data)
        object = s3.Object(os.environ.get('INPUT_BUCKET'), 'TestKey/anotherfile.txt')
        object.put(Body=more_binary_data)
        
        yield s3

def test_create_s3_uri(s3):
    """Test function for create_s3_uri() function in awsutils
    
    Parameters
    ----------
    s3 : Mocked S3 Instance
        Description
    """
    s3_base = awsu.S3Base()
    
    bucket = 'TestBucket'
    target_key =  'test_key'
    file = 'test_file'
    file_extension = 'csv'
    
    s3_uri_with_extension = s3_base.create_s3_uri(bucket, target_key, file, file_extension)
    assert s3_uri_with_extension == 's3://TestBucket/test_key/test_file.csv'
    
    s3_uri_without_extension = s3_base.create_s3_uri(bucket, target_key, file)
    assert s3_uri_without_extension == 's3://TestBucket/test_key/test_file'

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
    
    bucket = 'INPUT'
    true_key = 'TestKey/somefile.txt'
    fake_key = 'abc/def/ghi.csv'
    
    exists = s3_base.check_if_object_exists(bucket, true_key)
    doesnt_exist = s3_base.check_if_object_exists(bucket, fake_key)
    
    assert exists is True
    assert doesnt_exist is False

def test_list_buckets(s3):
    """Test function for list_buckets() function in awsutils
    
    Parameters
    ----------
    s3 : Mocked S3 Instance
        Description
    """
    s3_base = awsu.S3Base()
    buckets = s3_base.list_buckets()
    
    assert 'CONF' in buckets
    assert 'OUTPUT' in buckets
    assert 'LOG' in buckets

def test_list_keys(s3):
    """Test function for list_keys() function in awsutils
    
    Parameters
    ----------
    s3 : Mocked S3 Instance
        Description
    """
    s3_base = awsu.S3Base()
    bucket = 'INPUT'
    prefix =  'TestKey'
    keys = s3_base.list_keys(bucket, prefix)
    
    assert len(keys) == 2
    assert 'TestKey/somefile.txt' in keys
    assert 'TestKey/anotherfile.txt' in keys

def test_load_list_file_from_s3(s3):
    """Test function for load_list_file_from_s3() function in awsutils
    
    Parameters
    ----------
    s3 : Mocked S3 Instance
        Description
    """
    s3_base = awsu.S3Base()
    bucket = 'INPUT'
    target_key =  'TestKey/somefile.txt'
    
    content_lines = s3_base.load_list_file_from_s3(bucket,target_key)
    
    assert len(content_lines) == 2
    assert 'Here we have some data' in content_lines
    assert 'and this is a new line' in content_lines

def test_load_file_from_s3(s3):
    """Test function for load_file_from_s3() function in awsutils
    
    Parameters
    ----------
    s3 : Mocked S3 Instance
        Description
    """
    s3_base = awsu.S3Base()
    bucket = 'INPUT'
    target_key =  'TestKey/somefile.txt'
    
    content = s3_base.load_file_from_s3(bucket,target_key)
    
    assert content == b'Here we have some data\nand this is a new line'

def test_load_json_from_s3(s3):
    """Test function for load_json_from_s3() function in awsutils
    
    Parameters
    ----------
    s3 : Mocked S3 Instance
        Description
    """
    s3_base = awsu.S3Base()
    
    bucket = 'INPUT'
    target_key = 'TestKey/1111111111.json'

    
    with open("test/res/test_data.json") as f:
        body = f.read()
    
    s3.Object(bucket, target_key).put(Body=body)

    content = s3_base.load_json_from_s3(bucket,target_key)
    
    assert len(content) == 3
    assert content['1']['col2'] == 'abc'
    assert content['2']['col1'] == 2
    assert content['3']['col2'] == 'mno'

def test_upload_parquet_to_s3(s3):
    """Test function for upload_parquet_to_s3() function in awsutils
    
    Parameters
    ----------
    s3 : Mocked S3 Instance
        Description
    """
    s3_base = awsu.S3Base()
    
    bucket = 'INPUT'
    target_key = 'TestKey/1111111111.parquet'
    s3_uri = os.path.join(f's3://{bucket}',target_key)
    
    data = {'col1': [1, 2], 'col2': ['abc', 'def']}
    parquet_context = pd.DataFrame(data)

    s3_base.upload_parquet_to_s3(s3_uri, parquet_context)
    
    dataset = pq.ParquetDataset(s3_uri, filesystem= s3_base.s3_fs)
    table = dataset.read()
    df = table.to_pandas() 
    
    assert 'col1' in df.columns
    assert 'col2' in df.columns
    assert df['col1'][0] == 1
    assert df['col2'][1] == 'def'

def test_upload_as_json_to_s3(s3):
    """Test function for upload_as_json_to_s3() function in awsutils
     
    Parameters
    ----------
    s3 : Mocked S3 Instance
        Description
    """
    s3_base = awsu.S3Base()
    
    bucket = 'INPUT'
    target_key = 'TestKey/1111111111.json'

    
    with open("test/res/test_data.json") as f:
        content = f.read()
    content = json.loads(content)
    
    s3_base.upload_as_json_to_s3(content, bucket, target_key)
    
    body = s3.Object(bucket, target_key).get()['Body'].read().decode('utf-8')
    json_data = json.loads(body)

    assert len(json_data) == 3
    assert json_data['1']['col2'] == 'abc'
    assert json_data['2']['col1'] == 2
    assert json_data['3']['col2'] == 'mno'

def test_upload_as_csv_to_s3(s3, ssm):
    """Test function for upload_as_csv_to_s3() function in awsutils
     
    Parameters
    ----------
    s3 : Mocked S3 Instance
        Description
    """
    s3_base = awsu.S3Base()
    
    bucket = 'INPUT'
    target_key = 'TestKey/1111111111.csv'
    
    with open("test/res/test_data.csv") as f:
        content = f.read()
    
    s3_base.upload_as_csv_to_s3(content, bucket, target_key)
    
    body = s3.Object(bucket, target_key).get()['Body'].read().decode('utf-8')
    
    lines = body.splitlines()
    reader = csv.reader(lines, delimiter=',')
    data_list = list(reader)
    
    assert data_list[1][1] == 'abc'
    assert data_list[2][0] == '2'
    assert data_list[3][1] == 'ghi'

def test_upload_object_to_s3(s3):
    """Test function for upload_object_to_s3() function in awsutils
     
    Parameters
    ----------
    s3 : Mocked S3 Instance
        Description
    """
    s3_base = awsu.S3Base()
    
    bucket = 'INPUT'
    target_key = 'TestKey/1111111111.txt'
    body = 'some test data'
    
    s3_base.upload_object_to_s3(body, bucket, target_key)
    content = s3.Object(bucket, target_key).get()['Body'].read().decode('utf-8')
    
    assert content == 'some test data'

def test_delete_file_from_s3(s3):
    """Test function for delete_file_from_s3() function in awsutils
     
    Parameters
    ----------
    s3 : Mocked S3 Instance
        Description
    """
    s3_base = awsu.S3Base()
    
    bucket = 'INPUT'
    target_key = 'TestKey/somefile.txt'
    
    s3_bucket = s3.Bucket(bucket)
    objs = list(s3_bucket.objects.filter(Prefix=target_key))
    assert len(objs) > 0
    assert objs[0].key == target_key
    
    s3_base.delete_file_from_s3(bucket, target_key)
    s3_bucket = s3.Bucket(bucket)
    objs = list(s3_bucket.objects.filter(Prefix=target_key))
    
    assert not objs
    
def test_delete_all_keys_from_list(s3):
    """Test function for delete_all_keys_from_list() function in awsutils
     
    Parameters
    ----------
    s3 : Mocked S3 Instance
        Description
    """
    s3_base = awsu.S3Base()
    
    bucket = 'INPUT'
    prefix = 'TestKey'
    
    s3_bucket = s3.Bucket(bucket)
    objs = list(s3_bucket.objects.filter(Prefix=prefix))
    
    assert len(objs) == 2
    
    s3_base.delete_all_keys_from_list(bucket, prefix)
    s3_bucket = s3.Bucket(bucket)
    objs = list(s3_bucket.objects.filter(Prefix=prefix))
    
    assert not objs
