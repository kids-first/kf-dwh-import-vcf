#!/bin/bash
config=${1:-"config/production.conf"}
job_type=${2:-"clinvar"}
instance_count=${3:-"5"}
instance_type=${4:-"m5.2xlarge"}
env=${5:-"qa"}

aws s3 cp bootstrap-actions s3://kf-strides-variant-parquet-prd/jobs/bootstrap-actions --recursive
aws s3 cp documentation s3://kf-strides-variant-parquet-prd/jobs/documentation --recursive

bootstrapAction="no_bootstrap"

if [ ${job_type} == "clinvar" ]; then bootstrapAction="clinvar_ingestion"; fi
if [ ${job_type} == "orphanet" ]; then bootstrapAction="orphanet_ingestion"; fi
if [ ${job_type} == "ensembl_mapping" ]; then bootstrapAction="ensembl_ingestion"; fi

steps=$(cat <<EOF
[
  {
    "Args": [
      "spark-submit",
      "--packages","io.projectglow:glow-spark3_2.12:1.0.1",
      "--exclude-packages",
      "org.apache.httpcomponents:httpcore,org.apache.httpcomponents:httpclient",
      "--deploy-mode",
      "client",
      "--class", "org.kidsfirstdrc.dwh.external.ImportExternal",
      "s3a://kf-strides-variant-parquet-prd/jobs/${env}/kf-dwh-import-vcf.jar",
      "${config}",
      "${job_type}"
    ],
    "Type": "CUSTOM_JAR",
    "ActionOnFailure": "TERMINATE_CLUSTER",
    "Jar": "command-runner.jar",
    "Properties": "",
    "Name": "Spark application"
  }
]
EOF
)
instance_groups="[{\"InstanceCount\":${instance_count},\"InstanceGroupType\":\"CORE\",\"InstanceType\":\"${instance_type}\",\"Name\":\"Core - 2\"},{\"InstanceCount\":1,\"EbsConfiguration\":{\"EbsBlockDeviceConfigs\":[{\"VolumeSpecification\":{\"SizeInGB\":32,\"VolumeType\":\"gp2\"},\"VolumesPerInstance\":2}]},\"InstanceGroupType\":\"MASTER\",\"InstanceType\":\"m5.4xlarge\",\"Name\":\"Master - 1\"}]"

aws emr create-cluster --applications Name=Hadoop Name=Spark \
--ec2-attributes '{"KeyName":"flintrock","InstanceProfile":"kf-variant-emr-ec2-prd-profile","SubnetId":"subnet-031b7ef17a032fc3b","EmrManagedSlaveSecurityGroup":"sg-0d04e7c3ff5f36538","EmrManagedMasterSecurityGroup":"sg-0abad24e2a3e5e279"}' \
--service-role kf-variant-emr-prd-role \
--enable-debugging \
--release-label emr-6.3.0 \
--log-uri 's3n://kf-strides-variant-parquet-prd/jobs/elasticmapreduce/' \
--bootstrap-actions Path="s3://kf-strides-variant-parquet-prd/jobs/bootstrap-actions/${bootstrapAction}.sh" \
--steps "${steps}" \
--name "Import ${job_type}" \
--instance-groups "${instance_groups}" \
--scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
--auto-terminate \
--configurations file://./spark-config.json \
--region us-east-1
