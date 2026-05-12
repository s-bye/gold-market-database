#!/bin/bash

set -e

PYTHON=/var/etl/bin/python
ETL_DIR=/var/etl

echo "=============================="
echo "ETL start: $(date)"
echo "=============================="

echo "[1/3] collect_data..."
$PYTHON -m jupyter nbconvert --to notebook --execute \
    --inplace $ETL_DIR/collect_data.ipynb
echo "      collect_data done"

echo "[2/3] transform_data..."
$PYTHON -m jupyter nbconvert --to notebook --execute \
    --inplace $ETL_DIR/transform_data.ipynb
echo "      transform_data done"

echo "[3/3] load_to_mysql..."
$PYTHON -m jupyter nbconvert --to notebook --execute \
    --inplace $ETL_DIR/load_to_mysql.ipynb
echo "      load_to_mysql done"

echo "=============================="
echo "ETL done: $(date)"
echo "=============================="
