"""A library containing commonly used utils for AWS API
"""
import boto3
import botocore
import pandas as pd

class S3Base(object):

	"""S3Base class to handle the s3 requests

	Attributes
	----------
	s3_client : TYPE
			Description
	s3_resource : TYPE
			Description
	"""

	def __init__(self, config):
		"""Initialization of class with needed arguments for s3

		Parameters
		----------
		config : Dict
				Needed parameters for the s3
		"""
		self.s3_client = boto3.client('s3') if config['client'] else \
			config['client']
		self.s3_resource = boto3.resource('s3') if config['resource'] \
			else config['resource']

	def get_ssm_parameter(self, name):
		"""Returns the value of a parameter from ssm using the provided name

		Parameters
		----------
		name : String
				Name of the parameter

		Returns
		-------
		String
				Value of the parameter
		"""
		ssm_client = boto3.client('ssm', 'eu-central-1')
		obj = ssm_client.get_parameter(Name=name, WithDecryption=False)
		target_name = obj['Parameter']['Value'].encode()
		
		return target_name.decode('UTF-8')

	def upload_parquet_to_s3(self, s3_uri, parquet_context):
		"""Saves the provided Pandas Dataframe to the provided s3 URI in parquet format

		Parameters
		----------
		s3_uri : String
				S3 URI for parquet file
		parquet_context : Pandas DataFrame
				Data to be put in the file

		Returns
		-------
		String
				Description
		"""
		try:
			parquet_context.to_parquet(s3_uri, allow_truncated_timestamps=True)
			return f'file uploaded to {s3_uri}'
		except botocore.exceptions.ClientError as e:
			return f'couldn\'t upload to {s3_uri}, error: {e}'