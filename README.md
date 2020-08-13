KF DWH Import
=============

Spark Jobs to import VCF and others references databases into the datawarehosue.

HOWTO :

Run an import

```
./run_emr_job_import.sh SD_46SK55A3 RE_000004 variants m5d.4xlarge 20 "s3a://kf-study-us-east-1-prd-sd-46sk55a3/harmonized/family-variants/*postCGP.filtered.deNovo.vep.vcf.gz,s3a://bix-dev-data-bucket/PROJECTS/sd-46sk55a3_REHEADERED_TEMP/*.vcf.gz"
```

