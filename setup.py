from setuptools import setup

setup(
    name='data_utils',
    version='0.0.1',
    description='Private VNR package with most commonly used functions',
    license='VNR Verlag für die Deutsche Wirtschaft AG',
    url='git@github.com:vnrag/data-utils.git',
    packages=['data_utils'],
    install_requires=[
        'pandas',
        'json >= 1.0'
        'urllib3',
        'boto3',
        'botocore'
    ] #external packages as dependencies
)