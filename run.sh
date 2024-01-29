#!/bin/bash

set -eu

cd $(dirname "$0")
source venv/bin/activate

set -x

export PYSPARK_PYTHON=./venv/bin/python3
spark-submit \
    --name "Twitter spelling mistake analysis" \
    --master yarn \
    --archives pyspark_venv.tar.gz#venv \
    --py-files process.py,filter_tweets.py,visualise_analyse.py \
    --conf spark.dynamicAllocation.maxExecutors=20 \
    main.py


