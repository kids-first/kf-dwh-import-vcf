#!/bin/bash

wget https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz
aws s3 cp clinvar.vcf.gz ${1:-"s3://kf-strides-variant-parquet-prd"}/raw/clinvar/clinvar.vcf.gz