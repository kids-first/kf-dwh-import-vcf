#!/bin/bash
runEnv=${1:-"DEV"}
instance_count=${2:-"5"}
instance_type=${3:-"m5.xlarge"}

steps=$(cat <<EOF
[
  {
    "Args": [
      "spark-submit",
      "--packages","io.projectglow:glow_2.12:0.5.0",
      "--exclude-packages",
      "org.apache.httpcomponents:httpcore,org.apache.httpcomponents:httpclient",
      "--deploy-mode",
      "client",
      "--class", "org.kidsfirstdrc.dwh.external.ImportOrphanet",
      "s3a://kf-strides-variant-parquet-prd/jobs/kf-dwh-import-vcf.jar",
      "s3a://kf-strides-variant-parquet-prd/raw/orphanet",
      "s3a://kf-strides-variant-parquet-prd",
      "${runEnv}"
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
instance_groups="[{\"InstanceCount\":${instance_count},\"InstanceGroupType\":\"CORE\",\"InstanceType\":\"${instance_type}\",\"Name\":\"Core - 2\"},{\"InstanceCount\":1,\"EbsConfiguration\":{\"EbsBlockDeviceConfigs\":[{\"VolumeSpecification\":{\"SizeInGB\":32,\"VolumeType\":\"gp2\"},\"VolumesPerInstance\":2}]},\"InstanceGroupType\":\"MASTER\",\"InstanceType\":\"m5.xlarge\",\"Name\":\"Master - 1\"}]"

aws emr create-cluster --applications Name=Hadoop Name=Spark \
--ec2-attributes '{"KeyName":"flintrock","InstanceProfile":"kf-variant-emr-ec2-prd-profile","SubnetId":"subnet-031b7ef17a032fc3b","EmrManagedSlaveSecurityGroup":"sg-0d04e7c3ff5f36538","EmrManagedMasterSecurityGroup":"sg-0abad24e2a3e5e279"}' \
--service-role kf-variant-emr-prd-role \
--enable-debugging \
--release-label emr-6.2.0 \
--log-uri 's3n://kf-strides-variant-parquet-prd/jobs/elasticmapreduce/' \
--bootstrap-actions Path="s3://kf-strides-variant-parquet-prd/jobs/bootstrap-actions/orphanet_ingestion.sh" \
--steps "${steps}" \
--name "Import Orphanet ${runEnv}" \
--instance-groups "${instance_groups}" \
--scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
--auto-terminate \
--configurations file://./spark-config.json \
--region us-east-1
