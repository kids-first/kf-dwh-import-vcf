#!/bin/bash
job_type=${1:-"
"}
release_id=${2:-"re_000017"}
schema=${3:-"variant"}
number_instance=${4:-"15"}
instance_type=${5:-"m5.4xlarge"}
env=${6:-"prd"}

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
      "--class",
      "org.kidsfirstdrc.dwh.es.index.PrepareIndex",
      "s3a://kf-strides-variant-parquet-prd/jobs/${env}/kf-dwh-import-vcf.jar",
      "${job_type}",
      "${release_id}",
      "${schema}"
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
--release-label emr-6.3.0 \
--log-uri 's3n://kf-strides-variant-parquet-prd/jobs/elasticmapreduce/' \
--steps "${steps}" \
--name "manual - Prepare Index ${job_type} - ${release_id}" \
--instance-groups "${instance_groups}" \
--scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
--auto-terminate \
--configurations file://./spark-config.json \
--region us-east-1

