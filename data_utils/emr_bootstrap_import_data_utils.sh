#!/bin/bash
set -x -e
sudo yum update -y
sudo yum install -y git python3-pip
sudo ln -sf /usr/bin/python3 /usr/bin/python
sudo pip-3.6 install git+https://git@github.com/vnrag/data-utils.git@iwx_sequential_add