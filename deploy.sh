#!/bin/bash
tag=$1
folder=${2:-"qa"}
bucket=${3:-"s3://kf-strides-variant-parquet-prd"}

set -x
mkdir -p ~/.ivy2 ~/.sbt ~/.m2 ~/.sbt_cache
docker run --rm -v $(pwd):/app/build \
    --user $(id -u):$(id -g) \
    -v ~/.m2:/app/.m2 \
    -v ~/.ivy2:/app/.ivy2 \
    -v ~/.sbt:/app/.sbt \
    -v ~/.sbt_cache:/app/.cache \
    -w /app/build hseeberger/scala-sbt:11.0.10_1.5.2_2.12.13 \
    sbt -Duser.home=/app clean assembly

aws s3 cp target/scala-2.12/kf-dwh-import-vcf.jar ${bucket}/jobs/${folder}/kf-dwh-import-vcf.jar
aws s3 cp target/scala-2.12/kf-dwh-import-vcf.jar ${bucket}/jobs/${folder}/kf-dwh-import-vcf-${tag}.jar

aws s3 cp bin/documentation ${bucket}/jobs/documentation --recursive

aws s3 cp src/main/resources/config ${bucket}/jobs/${folder}/config --recursive