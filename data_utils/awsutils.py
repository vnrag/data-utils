"""A library containing commonly used utils for AWS API
"""
import os
import boto3
import botocore
import pandas as pd
import logging
import json
from data_utils.settings import Settings
from s3fs import S3FileSystem

class S3Base(object):

	"""S3Base class to handle the s3 requests

	Attributes
	----------
	s3_client : TYPE
			Description
	s3_resource : TYPE
			Description
	"""
	s3_conn = None
	settings = Settings()
	logger = None
	s3 = None

	def __init__(self):
		"""Initialization of class with needed arguments for s3

		Parameters
		----------
		config : Dict
				Needed parameters for the s3
		"""
		self.s3_connect()
		self.logger = logging.getLogger()
		self.logger.addHandler(logging.StreamHandler())
		self.logger.setLevel(logging.CRITICAL)
		logging.getLogger('boto3').setLevel(logging.CRITICAL)
		logging.getLogger('botocore').setLevel(logging.CRITICAL)
		logging.getLogger('s3transfer').setLevel(logging.CRITICAL)
		logging.getLogger('urllib3').setLevel(logging.CRITICAL)
	
	def s3_connect(self):
		"""
		Wrapper to create a session at AWS S3 with given authorization-key
		"""
		if not self.s3_conn and self.settings.local_docker:
			session = boto3.Session(
			aws_access_key_id=self.settings.AWS_ACCESS_KEY_ID,
			aws_secret_access_key=self.settings.AWS_SECRET_ACCESS_KEY
			)
			self.s3_conn = session.resource('s3', region_name=self.settings.AWS_REGION)
			if not self.s3_conn:
				raise ValueError('Invalid Region Name: {}'.format(self.settings.AWS_REGION))
			self.s3 = S3FileSystem()
		elif not self.s3_conn:
			session = boto3.Session()
			self.s3_conn = session.resource('s3')
			self.s3 = S3FileSystem()
		else:
			print ('Sorry, we do not have any information which enviorment to use, check local_docker Enviroment variable or if you have access rights to .aws credentials')

	def create_bucket(self,bucket):
		"""
		Wrapper to create a bucket in S3.
		"""
		self.s3_conn.create_bucket(Bucket=bucket,CreateBucketConfiguration={
			'LocationConstraint': self.settings.AWS_REGION
		})
		
	def check_if_object_exists(self, bucket, key):
		'''
		Wrapper for checking whether a given bucket already exists or not.
		return -- True: bucket exists
				  False: bucket does not exist
		'''
		bucket = self.s3_conn.Bucket(bucket)
		objs = list(bucket.objects.filter(Prefix=key))
		if len(objs) > 0 and objs[0].key == key:
			return True
		else:
			return False
	
	def get_ssm_parameter(self, name, encoded= False):
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
		target_name = obj['Parameter']['Value']
		
		return target_name.encode() if encoded else target_name

	def list_buckets(self):
		"""
		Wrapper to print a list of current buckets.
		"""
		for bucket in self.s3_conn.buckets.all(): 
			print (bucket.name)

	def list_keys(self, bucket, key=None):
		"""
		Wrapper to print a list of bucket-keys.
		"""
		bucket = self.s3_conn.Bucket(bucket)
		keys_list=[]
		if key:
			for obj in bucket.objects.filter(Prefix=key):
				keys_list.append(obj.key)
			return keys_list
		else:
			return []
	
	def list_all_partitions(self, bucket, partial_key):
		'''
		Wrapper for getting list of partitions from S3 bucket.
		return -- objectlist of file content
		'''
		response = self.s3_conn.list_objects(
    	Bucket=bucket,
    	Prefix=partial_key
		)
		keys_list = []
		if	(len(response['Contents'])>0):
			for i in range(0,len(response['Contents'])):
				keys_list.append(response['Contents'][i]['Key'])
			return keys_list
		else:
			return False
	
	def load_list_file_from_s3(self,bucket,key):
		'''
		Wrapper for getting file list with key from S3 bucket.
		return -- objectlist of file content
		'''
		try:
			if self.check_if_object_exists(bucket, key):
				bucket = self.s3_conn.Bucket(bucket)
				object = bucket.Object(key)
				return object.get()['Body'].read().decode('utf-8').split('\n')
			else: 
				return None
		except botocore.exceptions.ClientError as e:
			logging.warning (f'The following error occured while loading {key} from bucket {bucket}: {e}')
			if e.response['Error']['Code'] == "404":
				logging.warning("The object does not exist.")
			return []	    

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
	
	def upload_as_jsonp_to_s3(self, json_context, bucket, key):
		'''
		Wrapper to upload a jsonp-file with key to S3 bucket.
		'''
		# Uploads the given file using a managed uploader, which will split up large
		# files automatically and upload parts in parallel.
		# self.s3_conn.Object(bucket, key).put(Body=json_context, 'rb')
		self.upload_object_to_s3(json_context, bucket, key)

	def upload_as_json_to_s3(self, json_context, bucket, key):
		'''
		Wrapper to upload json-file with key to S3 bucket.
		'''
		# converting json object to string
		json_string = json.dumps(json_context)
		self.upload_object_to_s3(json_string, bucket, key)

	def upload_as_csv_to_s3(self, csv_context, bucket, key):
		'''
		Wrapper to upload a csv-file with key to a S3 bucket.
		'''
		# converting json object to string
		
		self.upload_object_to_s3(csv_context, bucket, key)

	def upload_object_to_s3(self, body, bucket, key):
		'''
		Wrapper to upload an unspecified file with key to a S3 bucket.
		'''
		# Uploads the given file using a managed uploader, which will split up large
		# files automatically and upload parts in parallel.
		
        #self.s3_conn.meta.client.upload_fileobj(data, bucket, key)
		self.s3_conn.Object(bucket, key).put(Body=body)
	
	
	def load_file_from_s3(self,bucket,key):
		'''
		Wrapper for loading a file with key from S3 bucket.
		return -- object of file content
		'''
		try:
			if self.check_if_object_exists(bucket, key):
				bucket = self.s3_conn.Bucket(bucket)
				object = bucket.Object(key)
				return object.get()['Body'].read()
			else: 
				return None
		except botocore.exceptions.ClientError as e:
			logging.warning ("The following error occured while loading %s from bucket %s: %s" % (key,bucket,e))
			if e.response['Error']['Code'] == "404":
				logging.warning("The object does not exist.")
			return None

	def load_json_from_s3(self, bucket, key):
		'''
		Wrapper for loading a json-file with key form given S3 bucket.
		return -- object of json file
		'''
		if self.check_if_object_exists(bucket, key):
			binary_data = self.load_file_from_s3(bucket,key)
		else: 
			return None

		if binary_data:
			try:
				return json.loads(binary_data.decode('utf-8'))
			except Exception as e:
				logging.critical ("could not convert %s, Exception: %s" % (key,e))
		else:
			return None

	def delete_file_from_s3(self,bucket,key):
		'''
		Wrapper for deleteing a file with key from given S3 bucket.
		'''
		self.s3_conn.Object(bucket,key).delete()

	def delete_all_keys_from_list(self, bucket, key):
		'''
		Wrapper for deleteing file from list of keys for given S3 bucket.
		'''
		key_list = self.list_keys(bucket, key=key)
		if key_list:
			for key in key_list:
				self.delete_file_from_s3(bucket, key)
				print("Key file: %s is deleted" % key)
			print("Master data for this data type is deleted")
		else:
			print("Master data for this data type is empty")
		

	def delete_all_keys_from_s3(self,bucket):
		'''
		Wrapper for deleting all files from a given S3 bucket.
		'''
		if self.settings.DEV:
			bucket = self.s3_conn.Bucket(bucket)
			bucket.objects.all().delete()
			bucket.delete()