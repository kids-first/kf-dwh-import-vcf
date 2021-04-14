KF DWH Import
=============

Spark Jobs to import VCF and others references databases into the datawarehosue.

### HOWTO :

#### Build assembly Jar

```
sbt assembly
```

#### Upload Jar to s3
note that the current version can be found in `kf-dwh-import-vcf/version.sbt`

```shell
aws s3 cp target/scala-2.12/kf-dwh-import-vcf-$VERSION.jar s3://kf-strides-variant-parquet-prd/jobs/kf-dwh-import-vcf-$VERSION.jar
```

#### Run an import

```
./run_emr_job_import.sh SD_46SK55A3 RE_000004 variants m5d.4xlarge 20 "s3a://kf-study-us-east-1-prd-sd-46sk55a3/harmonized/family-variants/*postCGP.filtered.deNovo.vep.vcf.gz,s3a://bix-dev-data-bucket/PROJECTS/sd-46sk55a3_REHEADERED_TEMP/*.vcf.gz"
```

#### Run a join
```
./run_emr_job_join.sh SD_46SK55A3,SD_6FPYJQBR,SD_9PYZAHHE,SD_DYPMEHHF,SD_DZTB5HRR,SD_PREASA7S,SD_R0EPRSGS,SD_YGVA0E1C RE_000004
```

#### Run variant_centric
```
./run_emr_job_prepare_index.sh variant_centric RE_000010
```

#### Run scalastyle

```
sbt scalastyle
```

