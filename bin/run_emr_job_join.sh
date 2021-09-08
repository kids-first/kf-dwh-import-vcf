#!/bin/bash
study_ids=${1:-"SD_46SK55A3,SD_9PYZAHHE,SD_DYPMEHHF,SD_BHJXBDQK,SD_7NQ9151J,SD_NMVV8A1Y,SD_0TYVY1TW,SD_Z6MWD3H0"}
release_id=${2:-"RE_000012"}
job=${3:-"variants"}
mergeExisting=${4:-"false"}
schema=${5:-"portal"}
number_instance=${6:-"15"}
instance_type=${7:-"r5.4xlarge"}
env=${8:-"qa"}

#steps="[{\"Args\":[\"spark-submit\",\"--deploy-mode\",\"client\",\"--class\",\"org.kidsfirstdrc.dwh.join.Join\",\"s3a://kf-strides-variant-parquet-prd/jobs/kf-dwh-import-vcf.jar\",\"${study_ids}\",\"${release_id}\",\"s3a://kf-strides-variant-parquet-prd\",\"${job}\",\"${mergeExisting}\",\"variant\"],\"Type\":\"CUSTOM_JAR\",\"ActionOnFailure\":\"TERMINATE_CLUSTER\",\"Jar\":\"command-runner.jar\",\"Properties\":\"\",\"Name\":\"Spark application\"}]"
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
      "org.kidsfirstdrc.dwh.join.Join",
      "s3a://kf-strides-variant-parquet-prd/jobs/${env}/kf-dwh-import-vcf.jar",
      "${study_ids}",
      "${release_id}",
      "${job}",
      "${mergeExisting}",
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
--name "Join ${job} - ${study_ids} - ${release_id} - ${schema}" \
--instance-groups "${instance_groups}" \
--scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
--auto-terminate \
--configurations file://./spark-config.json \
--region us-east-1

