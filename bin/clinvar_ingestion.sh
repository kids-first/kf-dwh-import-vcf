#!/bin/bash
wget https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz
aws s3 cp clinvar.vcf.gz s3://kf-strides-variant-parquet-prd/raw/clinvar/clinvar_$(date +'%Y%m%d').vcf.gz
