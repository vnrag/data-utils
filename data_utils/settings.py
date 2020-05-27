from pathlib import Path
import os
import sys
from os.path import join, dirname
from dotenv import load_dotenv

class Settings(object):
  HOMDIR = str(Path.home())
  BASEDIR = os.path.abspath(os.path.dirname(__file__))
  AWS_ACCESS_KEY_ID = None
  AWS_SECRET_ACCESS_KEY = None
  AWS_REGION = None

  EXPORT_BUCKET = None
  DATA_INPUT = None
  DATA_OUTPUT = None

  DB_CSV = None

  USERNAME_DB = None
  PASSWORD_DB = None
  HOSTNAME_DB = None
  PORT_DB = None
  DATABASE_DB = None
 
  USERNAME_DB_MDB = None
  PASSWORD_DB_MDB = None
  HOSTNAME_DB_MDB = None
  PORT_DB_MDB = None
  DATABASE_DB_MDB = None
  local_docker=True
  HOME_DIR=None
  CHUNK_SIZE = 100000
  PROD = False

  def __init__(self, env_file=True, local_docker=True):

    if env_file:
      dotenv_path = join(self.BASEDIR,'.env')
      print(dotenv_path)
      load_dotenv(dotenv_path)

    self.EXPORT_BUCKET = os.getenv("EXPORT_BUCKET")
    self.DATA_INPUT = os.getenv("DATA_INPUT")
    self.DATA_OUTPUT = os.getenv("DATA_OUTPUT")
    self.DB_CSV = self.str2bool(os.getenv("DB_CSV"))
    self.CHUNK_SIZE = os.getenv("CHUNK_SIZE")
    self.PROD = self.str2bool(os.getenv("PROD"))
    self.USERNAME_DB = os.getenv("USERNAME_DB")
    self.PASSWORD_DB = os.getenv("PASSWORD_DB")
    self.HOSTNAME_DB = os.getenv("HOSTNAME_DB")
    self.PORT_DB = os.getenv("PORT_DB")
    self.DATABASE_DB = os.getenv("DATABASE_DB")

    self.USERNAME_DB_MDB = os.getenv("USERNAME_DB_MDB")
    self.PASSWORD_DB_MDB = os.getenv("PASSWORD_DB_MDB")
    self.HOSTNAME_DB_MDB = os.getenv("HOSTNAME_DB_MDB")
    self.PORT_DB_MDB = os.getenv("PORT_DB_MDB")
    self.DATABASE_DB_MDB = os.getenv("DATABASE_DB_MDB")

    if local_docker:
      self.HOME_DIR=os.getenv("HOME_DIR")
      aws_credentials = join('home',self.HOMDIR, '.aws','credentials')
      load_dotenv(aws_credentials)
      self.AWS_ROLE=os.getenv("aws_access_key_id")
      self.AWS_ACCESS_KEY_ID=os.getenv("aws_access_key_id")
      self.AWS_SECRET_ACCESS_KEY=os.getenv("aws_secret_access_key")
      self.AWS_REGION=os.getenv("region")
    # Mapping for first part of path "mandator":"publishing-group"
    self.PUBLISHING_GROUP = {
      "VNR":"VNR",	# currently in etl
      "FKD":"VNR", # currently in etl
      "RCH":"VNR", # currently in etl
      "FID":"VNR", # deprecated - old mandator. Maybe we will have old contracts in it
      "wbd":"weltbild", # currently not in etl
      "wbc":"weltbild", # currently not in etl
      "dpv":"dpv", # currently not in etl
      "dck":"dck", # currently not in etl
      "GENERAL":"VNR"	# currently in etl
    }

  def str2bool(self, v):
    return str(v).lower() in ("yes", "true", "t", "1", True)