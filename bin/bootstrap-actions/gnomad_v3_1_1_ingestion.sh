#!/bin/bash

function download_and_upload_to_S3() {
  chr=$1
  filename=gnomad.genomes.v3.1.1.sites.${chr}.vcf
  wget https://storage.googleapis.com/gcp-public-data--gnomad/release/3.1.1/vcf/genomes/${filename}.bgz
  aws s3 cp ${filename}.bgz s3://kf-strides-variant-parquet-prd/raw/gnomad/r3.1.1/${filename}.gz
  rm ${filename}.bgz
}

download_and_upload_to_S3 chr1
download_and_upload_to_S3 chr2
download_and_upload_to_S3 chr3
download_and_upload_to_S3 chr4
download_and_upload_to_S3 chr5
download_and_upload_to_S3 chr6
download_and_upload_to_S3 chr7
download_and_upload_to_S3 chr8
download_and_upload_to_S3 chr9
download_and_upload_to_S3 chr10
download_and_upload_to_S3 chr11
download_and_upload_to_S3 chr12
download_and_upload_to_S3 chr13
download_and_upload_to_S3 chr14
download_and_upload_to_S3 chr15
download_and_upload_to_S3 chr16
download_and_upload_to_S3 chr17
download_and_upload_to_S3 chr18
download_and_upload_to_S3 chr19
download_and_upload_to_S3 chr20
download_and_upload_to_S3 chr21
download_and_upload_to_S3 chr22
download_and_upload_to_S3 chrX
download_and_upload_to_S3 chrY
