#!/bin/bash
wget http://www.orphadata.org/data/xml/en_product6.xml
aws s3 cp en_product6.xml s3://kf-strides-variant-parquet-prd/raw/orphanet/en_product6.xml
wget http://www.orphadata.org/data/xml/en_product9_ages.xml
aws s3 cp en_product9_ages.xml s3://kf-strides-variant-parquet-prd/raw/orphanet/en_product9_ages.xml