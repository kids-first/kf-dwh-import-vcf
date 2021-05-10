#!/bin/bash

function download_ensembl_tsv() {
  release_number=$1
  file=$2
  filename=Homo_sapiens.GRCh38.${release_number}.$file.tsv.gz
  wget http://ftp.ensembl.org/pub/release-${release_number}/tsv/homo_sapiens/${filename}
  aws s3 cp ${filename} s3://kf-strides-variant-parquet-prd/raw/ensembl/${filename}
  rm $filename
}

download_ensembl_tsv 104 canonical
download_ensembl_tsv 104 refseq
download_ensembl_tsv 104 entrez
download_ensembl_tsv 104 uniprot
download_ensembl_tsv 104 ena