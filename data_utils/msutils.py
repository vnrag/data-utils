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


class MSUtils(object):
    """[summary]

    Args:
            object ([type]): [description]
    """
    ssm_conn = None
    logger = None
    download_path = None
    provider = "onedrive"

    def __init__(self, download_path, folder=False, external_sess=None):
        """[summary]

        Args:
                download_path ([type]): [description]
                folder (bool, optional): [description]. Defaults to False.
                external_sess ([type], optional): [description]. Defaults to None.
        """
        self.ssm_conn = external_sess if external_sess else self.ssm_connect()
        self.logger = logging.getLogger()
        self.logger.addHandler(logging.StreamHandler())
        self.logger.setLevel(logging.CRITICAL)
        logging.getLogger("urllib3").setLevel(logging.CRITICAL)
        logging.getLogger("onedrive_download").setLevel(logging.CRITICAL)

        self.download_path = download_path
        self.folder = folder

    # This function will later be removed and used in another repository, it is not part
    # of data utils, but now we can leave it here as integrated main function
    # you can use this also for creating the process flow with authentification and uploading
    def fetch_data_from_onedrive(self):
        """[summary]
        """
        one_drive_access_url = self.create_url_for_onedrive()
        auth_token = self.get_auth_token_onedrive_app()
        one_drive_path = self.build_one_drive_path()
        if self.folder:
            one_drive_file_list = []
            one_drive_file_list = self.get_all_elements_in_folder()
            for element in folder:
                try:
                    one_drive_file = self.download_element_from_onedrive(
                        one_drive_path, auth_token)
                    upload_key = self.transform_onedrive_folder_structure_to_s3_structure(
                        one_drive_path)
					### success_message = awsutils.upload_to_se(bucket, upload_key)
                    logging.info(f"successfully uploaded {one_drive_path} to s3: {success_message}")
                except Exception as e:
                    logging.warning(
                        f"error by processing {one_drive_path}: {e}")
        else:
            try:
                one_drive_file = self.download_element_from_onedrive(
                    one_drive_path, auth_token)
                upload_key = self.transform_onedrive_folder_structure_to_s3_structure(
                    one_drive_path)
					### success_message = awsutils.upload_to_se(bucket, upload_key)
                    logging.info(f"successfully uploaded {one_drive_path} to s3: {success_message}")
            except Exception as e:
                logging.warning(f"error by processing {one_drive_path}: {e}")

    def transform_onedrive_folder_structure_to_s3_structure(
            self, one_drive_path):
        """[summary]

        Args:
                one_drive_path ([type]): [description]
        """
        # use f"provider={self.provider}" always as prefix for the s3 object
        return None

    def create_url_for_onedrive(self):
        """[summary]
        """
        return None

    def get_auth_token_onedrive_app(self):
        """[summary]
        """

    def check_downloaded_csv_object(self):
        """[summary]
        """
        return None

    def build_one_drive_path(self):
        """[summary]
        """
        return None

    def download_element_from_onedrive(self, auth_token):
        """[summary]

        Args:
                auth_token ([type]): [description]
        """
        return None

    def get_all_elements_in_folder(self, one_drive_path):
        """[summary]

        Args:
                one_drive_path ([type]): [description]
        """
        return None
