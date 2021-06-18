#!/bin/bash

aws s3 cp s3://kf-strides-variant-parquet-prd/reference_genome/GRCh38_full_analysis_set_plus_decoy_hla.fa \
/home/hadoop/GRCh38_full_analysis_set_plus_decoy_hla.fa

aws s3 cp s3://kf-strides-variant-parquet-prd/reference_genome/GRCh38_full_analysis_set_plus_decoy_hla.fa.fai \
/home/hadoop/GRCh38_full_analysis_set_plus_decoy_hla.fa.fai


