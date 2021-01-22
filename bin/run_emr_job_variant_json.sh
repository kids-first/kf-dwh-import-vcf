#!/bin/bash
release_id=$1
input=${2:-"s3a://kf-strides-variant-parquet-prd"}
output=${3:-"s3a://kf-strides-variant-parquet-prd"}
jarV=${4:-"0.1"}
number_instance=${5:-"10"}
instance_type=${6:-"r5.4xlarge"}

steps=$(cat <<EOF
[
  {
    "Args": [
      "spark-submit",
      "--deploy-mode",
      "client",
      "--class",
      "org.kidsfirstdrc.dwh.variantDb.json.VariantsToJson",
      "s3a://kf-strides-variant-parquet-prd/jobs/kf-dwh-import-vcf-${jarV}.jar",
      "${release_id}",
      "${input}",
      "${output}"
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

instance_groups="[{\"InstanceCount\":${number_instance},\"BidPrice\":\"OnDemandPrice\",\"EbsConfiguration\":{\"EbsBlockDeviceConfigs\":[{\"VolumeSpecification\":{\"SizeInGB\":150,\"VolumeType\":\"gp2\"},\"VolumesPerInstance\":8}],\"EbsOptimized\":true},\"InstanceGroupType\":\"CORE\",\"InstanceType\":\"${instance_type}\",\"Name\":\"Core - 2\"},{\"InstanceCount\":1,\"EbsConfiguration\":{\"EbsBlockDeviceConfigs\":[{\"VolumeSpecification\":{\"SizeInGB\":32,\"VolumeType\":\"gp2\"},\"VolumesPerInstance\":2}]},\"InstanceGroupType\":\"MASTER\",\"InstanceType\":\"m5.xlarge\",\"Name\":\"Master - 1\"}]"

aws emr create-cluster --applications Name=Hadoop Name=Spark \
--ec2-attributes '{"KeyName":"flintrock","InstanceProfile":"kf-variant-emr-ec2-prd-profile","ServiceAccessSecurityGroup":"sg-0587a1d20e24f4104","SubnetId":"subnet-00aab84919d5a44e2","EmrManagedSlaveSecurityGroup":"sg-0dc6b48e674070821","EmrManagedMasterSecurityGroup":"sg-0a31895d33d1643da"}' \
--service-role kf-variant-emr-prd-role \
--enable-debugging \
--release-label emr-6.2.0 \
--log-uri 's3n://kf-strides-variant-parquet-prd/jobs/elasticmapreduce/' \
--steps "${steps}" \
--name "Variant index - Release ${release_id}" \
--instance-groups "${instance_groups}" \
--scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
--auto-terminate \
--configurations file://./spark-config.json \
--region us-east-1

