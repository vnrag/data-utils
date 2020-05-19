data_utils
--------

To install data_utils using pip, please run::

    >>> pip3 install git+ssh://git@github.com/vnrag/data-utils.git


To use any of the different libraries under data_utils in your code, simply do::

    >>> from data_utils import generalutils as gu
    >>> print(gu.get_unix_timestamp('2020-01-10'))

To update the sphinx documentation of data_utils, please run the following steps::

    >>> sphinx-apidoc -o "<<YOUR_PROJECT_FOLDER_PATH>>/data-utils/data_utils/documentation/rst/" "<<YOUR_PROJECT_FOLDER_PATH>>/data-utils/data_utils/" "<<YOUR_PROJECT_FOLDER_PATH>>/data-utils/data_utils/config/" --force
    >>> cd "<<YOUR_PROJECT_FOLDER_PATH>>/data-utils/data_utils/documentation"
    >>> make html
