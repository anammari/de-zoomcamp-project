#!/bin/bash

# install Google Python client on all nodes
apt-get -y update
apt-get install python-dev
apt-get install -y python-pip
pip install --upgrade google-api-python-client

# git clone on Master
USER=ahmad_ammari # the username that dataproc runs as
ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
if [[ "${ROLE}" == 'Master' ]]; then
  cd home/$USER
  git clone https://github.com/anammari/de-zoomcamp-project.git
  chown -R $USER de-zoomcamp-project
fi
