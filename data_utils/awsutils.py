import boto3
import botocore


class S3Base(object):
    '''[S3Base class to handle the s3 requests]
    '''

    def __init__(self, config):
        '''[Initialization of class with needed arguments for s3.]
        Arguments:
           args {[dict]} -- [Needed parameters for the s3.]
        '''
        self.s3_client = boto3.client('s3') if not config['client'] else \
                            config['client']
        self.s3_resource = boto3.resource('s3') if not config['resource'] \
                                else config['resource']

    def get_ssm_parameter(self, name):
        '''[summary]
        Arguments:
            name {[string]} -- [Name of the parameter.]
        Returns:
            [string] -- [Value for given name from s3 parameters .]
        '''
        ssm_client = boto3.client('ssm', 'eu-central-1')
        obj = ssm_client.get_parameter(
            Name=name,
            WithDecryption=False)
        return obj['Parameter']['Value']

    def upload_parquet_to_s3(self, s3_uri, parquet_context):
        ''' Writes provided json data to given s3
        Arguments:
            s3_uri {[string]} -- [S3 URI for parquet file]
            data {[dataframe} -- [Data to be put in the file.]
        '''
        try:
            parquet_context.to_parquet(s3_uri, allow_truncated_timestamps=True)
            return f'file uploaded to {s3_uri}'
        except botocore.exceptions.ClientError as e:
            return f'couldn\'t upload to {s3_uri}, error: {e}'