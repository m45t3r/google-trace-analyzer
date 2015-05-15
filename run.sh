#!/bin/bash

# Install gsutil and downloads Google trace data
if command -v gsutil &> /dev/null; then
  sudo pip install gsutil
fi
gsutil -m rsync -R gs://clusterdata-2011-2/ google_trace_data
make all
mkdir traces
# The command below will take lots of time, you may comment
# some functions after they finish to speed up the process
python TaskUsageUtils.py
