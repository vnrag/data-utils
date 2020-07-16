from setuptools import setup, find_packages

# Package meta-data.
NAME = 'data_utils'
DESCRIPTION = 'Public VNR package with most commonly used functions'
URL = 'git@github.com:vnrag/data-utils.git'
VERSION = '0.1.0'
REQUIRES_PYTHON = '>=3.6.0'


# What packages are required for this module to be executed?
REQUIRED = [
    'pandas',
    'urllib3',
    'boto3',
    'botocore',
    'pyarrow',
    's3fs',
    'docopt',
    'python-dotenv',
    'awswrangler'
]

# Import the README and use it as the long-description.
# Note: this will only work if 'README.rst' is present in your MANIFEST.in file!
try:
    with open("README.rst", 'r', encoding='utf-8') as f:
        long_description= f.read()
except FileNotFoundError:
    long_description = DESCRIPTION
   

setup(
    name=NAME,
    version=VERSION,
    description=DESCRIPTION,
    long_description=long_description,
    long_description_content_type='text/x-rst',
    python_requires=REQUIRES_PYTHON,
    url=URL,
    license='MIT',
    packages=find_packages(exclude=("test",)),
    install_requires=REQUIRED,
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3.7",
        'License :: OSI Approved :: MIT License',
        "Operating System :: OS Independent"
    ],
)
