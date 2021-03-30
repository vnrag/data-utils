"""A library containing commonly used utils for AWS API
"""
import os
import boto3
import botocore
import pandas as pd
import logging
import json
from data_utils import generalutils as gu
from s3fs import S3FileSystem
import awswrangler as wr


class SSMBase(object):
    """SSMBase class to handle the ssm requests
    """
    ssm_conn = None
    logger = None

    def __init__(self, external_sess=None):
        self.ssm_conn = external_sess if external_sess else self.ssm_connect()
        self.logger = gu.get_logger(__name__)

    def ssm_connect(self):
        session = boto3.Session()
        ssm_conn = session.client("ssm")
        return ssm_conn

    def get_ssm_parameter(self, name, encoded=False):
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
        obj = self.ssm_conn.get_parameter(Name=name, WithDecryption=False)
        target_name = obj["Parameter"]["Value"]

        return target_name.encode() if encoded else target_name


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
    logger = None
    s3_fs = None

    def __init__(self):
        """Initialization of class with needed arguments for s3

        Parameters
        ----------
        """
        self.s3_connect()
        self.logger = gu.get_logger(__name__)

    def s3_connect(self):
        """
        Wrapper to create a session at AWS S3 with given authorization-key
        """
        session = boto3.Session()
        self.s3_conn = session.resource("s3")
        self.s3_fs = S3FileSystem()

    def create_s3_uri(self, bucket, key, tmpFileName, FileType=None):
        """creates and s3 uri: s3://bucket/key

        Arguments:
                bucket {string} -- bucket name
                key {string} -- key path
        """
        return gu.get_target_path(["s3://", bucket, key, tmpFileName], FileType)

    def check_if_object_exists(self, bucket, key):
        """
        Wrapper for checking whether a given object already exists in a bucket or not.
        return -- True: object exists
                          False: object does not exist
        """
        bucket = self.s3_conn.Bucket(bucket)
        objs = list(bucket.objects.filter(Prefix=key))
        if len(objs) > 0 and objs[0].key == key:
            return True
        else:
            return False

    def list_buckets(self):
        """
        Wrapper to print a list of current buckets.
        """
        buckets = []
        for bucket in self.s3_conn.buckets.all():
            buckets.append(bucket.name)
        return buckets

    def list_keys(self, bucket, prefix=None):
        """
        Wrapper to print a list of bucket-keys.
        """
        bucket = self.s3_conn.Bucket(bucket)
        keys_list = []
        if prefix:
            for obj in bucket.objects.filter(Prefix=prefix):
                keys_list.append(obj.key)
            return keys_list
        else:
            return []

    def load_list_file_from_s3(self, bucket, key):
        """
        Wrapper for getting file list with key from S3 bucket.
        return -- objectlist of file content
        """
        try:
            if self.check_if_object_exists(bucket, key):
                bucket = self.s3_conn.Bucket(bucket)
                object = bucket.Object(key)
                return object.get()["Body"].read().decode("utf-8").split("\n")
            else:
                return None
        except botocore.exceptions.ClientError as e:
            self.logger.warning(
                f"The following error occured while loading {key} from bucket {bucket}: {e}")
            if e.response["Error"]["Code"] == "404":
                self.logger.warning("The object does not exist.")
            return []

    def load_file_from_s3(self, bucket, key):
        """
        Wrapper for loading a file with key from S3 bucket.
        return -- object of file content
        """
        try:
            if self.check_if_object_exists(bucket, key):
                bucket = self.s3_conn.Bucket(bucket)
                object = bucket.Object(key)
                return object.get()["Body"].read()
            else:
                return None
        except botocore.exceptions.ClientError as e:
            self.logger.warning(
                "The following error occured while loading %s from bucket %s: %s" % (key, bucket, e))
            if e.response["Error"]["Code"] == "404":
                self.logger.warning("The object does not exist.")
            return None

    def load_json_from_s3(self, bucket, key):
        """
        Wrapper for loading a json-file with key form given S3 bucket.
        return -- object of json file
        """
        if self.check_if_object_exists(bucket, key):
            binary_data = self.load_file_from_s3(bucket, key)
        else:
            return None

        if binary_data:
            try:
                return json.loads(binary_data.decode("utf-8"))
            except Exception as e:
                self.logger.critical(
                    "could not convert %s, Exception: %s" % (key, e))
        else:
            return None

    def load_parquet_with_wrangler(self, s3_uri):
        try:
            return wr.s3.read_parquet(s3_uri)
        except Exception as e:
            self.logger.critical(
                f"couldn't read paruqet file {s3_uri}, Exception {e}")

    def upload_parquet_with_wrangler(self, s3_uri, context):
        try:
            wr.s3.to_parquet(
                df=context,
                path=s3_uri
            )
            print(f"---- File uploaded to {s3_uri} ----")
        except botocore.exceptions.ClientError as e:
            self.logger.critical(f"couldn't upload to {s3_uri}, error: {e}")

    def upload_parquet_to_s3(self, s3_uri, parquet_context, use_deprecated_int96_timestamps=False):
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
            parquet_context.to_parquet(s3_uri, allow_truncated_timestamps=True,
                                       use_deprecated_int96_timestamps=use_deprecated_int96_timestamps)
            return f"file uploaded to {s3_uri}"
        except botocore.exceptions.ClientError as e:
            return f"couldn\"t upload to {s3_uri}, error: {e}"

    def upload_as_jsonp_to_s3(self, json_context, bucket, key):
        """
        Wrapper to upload a jsonp-file with key to S3 bucket.
        """
        # Uploads the given file using a managed uploader, which will split up large
        # files automatically and upload parts in parallel.
        # self.s3_conn.Object(bucket, key).put(Body=json_context, "rb")
        self.upload_object_to_s3(json_context, bucket, key)

    def upload_as_json_to_s3(self, json_context, bucket, key):
        """
        Wrapper to upload json-file with key to S3 bucket.
        """
        # converting json object to string
        json_string = json.dumps(json_context)
        self.upload_object_to_s3(json_string, bucket, key)

    def upload_as_csv_to_s3(self, csv_context, bucket, key):
        """
        Wrapper to upload a csv-file with key to a S3 bucket.
        """
        # converting json object to string

        self.upload_object_to_s3(csv_context, bucket, key)

    def upload_object_to_s3(self, body, bucket, key):
        """
        Wrapper to upload an unspecified file with key to a S3 bucket.
        """
        # Uploads the given file using a managed uploader, which will split up large
        # files automatically and upload parts in parallel.

    #self.s3_conn.meta.client.upload_fileobj(data, bucket, key)
        self.s3_conn.Object(bucket, key).put(Body=body)

    def delete_file_from_s3(self, bucket, key):
        """
        Wrapper for deleteing a file with key from given S3 bucket.
        """
        self.s3_conn.Object(bucket, key).delete()

    def delete_all_keys_from_list(self, bucket, prefix):
        """
        Wrapper for deleteing file from list of keys for given S3 bucket.
        """
        key_list = self.list_keys(bucket, prefix=prefix)
        if key_list:
            for key in key_list:
                self.delete_file_from_s3(bucket, key)
                print("Key file: %s is deleted" % key)
            print("Master data for this data type is deleted")
        else:
            print("Master data for this data type is empty")

    def create_data_key(self, partition_date, file_name, data_type, mandator, personal=False,  parquet=None, month=False, report=False):
        """
        Creates entire Filename with for given partition_date and data_type including path.

        return -- filename for data_type and partition_date with path
        """
        # platformx.data.etl.raw/{publishing_group}/{mandator}/{year}/{month}/{day}/
        if parquet:
            time_partition = gu.create_time_partition(partition_date)
            return gu.get_target_path([f"personal={personal}", f"transaction={data_type}", f"partition_mandator={mandator}", time_partition])
        elif report:
            time_partition = gu.create_time_partition(partition_date, month)
            return gu.get_target_path(["report", "partition",  f"mandator={mandator}", time_partition, file_name])
        else:
            time_partition = gu.create_time_partition(partition_date, month)
            return gu.get_target_path([f"personal={personal}", f"transaction={data_type}", f"partition_mandator={mandator}", time_partition, file_name])

    def store_raw_data_in_s3(self, partition_date, data_type, data, personal, mandator, export_bucket, parquet=False, chunk=0, chunk_name=False, use_deprecated_int96_timestamps=False):
        """
        Uploads for given data_type and partition_date corresponding data as raw json or parquet file_name in S3 bucket.
        """
        if parquet:
            file_name = str(data_type)
            data_key = self.create_data_key(partition_date, str(
                data_type), data_type, mandator, personal=personal, parquet=parquet)
            if chunk_name:
                file_name = f"{str(chunk)}_{file_name}"
            s3_uri = self.create_s3_uri(
                export_bucket, data_key, file_name, FileType="parquet")
            self.upload_parquet_to_s3(s3_uri, data, use_deprecated_int96_timestamps)
        else:
            data_key = self.create_data_key(
                partition_date, f"{str(data_type)}.jsonp", data_type, mandator, personal=personal)
            self.upload_as_jsonp_to_s3(data, export_bucket, data_key)

    def create_report_log_key(self, process_start_date, export_type, index, data_type, mandator):
        '''
        Creates report-log-key including path and filename for given data_type.

        return -- path and filename for the report-log for data_type
        '''
        time_partition = gu.create_time_partition(process_start_date)
        return gu.get_target_path([f"type={export_type}", time_partition, f"{data_type}_{mandator}_{str(index)}_{process_start_date.strftime('%H%M%S')}.json"])

    def store_report_log_in_s3(self, export_type, data, process_start_date, index, data_type, mandator, log_bucket=None):
        '''
        Uploads the corresponding report-log for the data-type in S3 log-bucket.
        '''
        if log_bucket:
            report_log_key = self.create_report_log_key(
                process_start_date, export_type, index, str(data_type), mandator)
            self.upload_as_json_to_s3(data, log_bucket, report_log_key)
        else:
            print('No log-bucket defined')

    def create_partial_data_key_deletion(self, data_type, mandantor, personal=False, parquet=None):
        '''
        Creates entire partial Filename for given data_type including path.

        return -- partial filename for data_type with path to delete files under this directory
        '''
        return gu.get_target_path([f"personal={str(personal)}", f"master_data={data_type}", f"partition_mandator={mandantor}"])

    def delete_raw_master_data_in_s3(self, data_type, personal, export_bucket, mandantor, parquet=None):
        '''
        delete for given data_type corresponding master data as raw file_name in S3 bucket.
        '''
        if parquet:
            partial_data_key_deletion = self.create_partial_data_key_deletion(
                data_type, mandantor, personal=personal, parquet=parquet)
            self.delete_all_keys_from_list(
                export_bucket, partial_data_key_deletion)
        else:
            partial_data_key_deletion = self.create_partial_data_key_deletion(
                data_type, mandantor, personal=personal)
            self.delete_all_keys_from_list(
                export_bucket, partial_data_key_deletion)

    def create_data_key_dimension(self, file_name, data_type, chunk, mandator, personal=False, parquet=False):
        '''
        Creates entire Filename for given data_type including path.

        return -- filename for data_type with path
        '''
        if parquet:
            return gu.get_target_path([f"personal={str(personal)}", f"master_data={data_type}", f"partition_mandator={mandator}", f"partition_chunk={chunk}"])
        else:
            return gu.get_target_path([f"personal={str(personal)}", f"master_data={data_type}", f"partition_mandator={mandator}", f"partition_chunk={chunk}", file_name])

    def store_raw_master_data_in_s3(self, data_type, file_name, data, personal, chunk, export_bucket, mandator, parquet=None, use_deprecated_int96_timestamps=False):
        '''
        Uploads for given data_type corresponding master data as raw json or parquet file_name in S3 bucket.
        '''
        if parquet:
            data_key = self.create_data_key_dimension(
                f"{str(file_name)}.parquet", data_type, chunk, mandator, personal=personal, parquet=True)
            s3_uri = self.create_s3_uri(
                export_bucket, data_key, str(file_name), FileType='parquet')
            self.upload_parquet_to_s3(s3_uri, data, use_deprecated_int96_timestamps)
        else:
            data_key = self.create_data_key_dimension(
                "{str(file_name)}.jsonp", data_type, chunk, mandator, personal=personal)
            self.upload_as_jsonp_to_s3(data, export_bucket, data_key)

    def store_custom_data_in_s3_csv(self, export_bucket, key, data):
        '''
        Uploads for given fact_type corresponding data as csv file_name in S3 bucket.
        '''
        self.upload_as_csv_to_s3(data, export_bucket, key)
