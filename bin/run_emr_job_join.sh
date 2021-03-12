#!/bin/bash
study_ids=$1
release_id=$2
job=${3:-"all"}
mergeExisting=${4:-"true"}
number_instance=${5:-"20"}
instance_type=${6:-"r5.4xlarge"}

#steps="[{\"Args\":[\"spark-submit\",\"--deploy-mode\",\"client\",\"--class\",\"org.kidsfirstdrc.dwh.join.Join\",\"s3a://kf-strides-variant-parquet-prd/jobs/kf-dwh-import-vcf.jar\",\"${study_ids}\",\"${release_id}\",\"s3a://kf-strides-variant-parquet-prd\",\"${job}\",\"${mergeExisting}\",\"variant\"],\"Type\":\"CUSTOM_JAR\",\"ActionOnFailure\":\"TERMINATE_CLUSTER\",\"Jar\":\"command-runner.jar\",\"Properties\":\"\",\"Name\":\"Spark application\"}]"
steps=$(cat <<EOF
[
  {
    "Args": [
      "spark-submit",
      "--packages","io.projectglow:glow_2.12:0.5.0,bio.ferlab:datalake-lib_2.12:0.0.2",
      "--exclude-packages",
      "org.apache.httpcomponents:httpcore,org.apache.httpcomponents:httpclient",
      "--deploy-mode",
      "client",
      "--class",
      "org.kidsfirstdrc.dwh.join.Join",
      "s3a://kf-strides-variant-parquet-prd/jobs/kf-dwh-import-vcf.jar",
      "${study_ids}",
      "${release_id}",
      "s3a://kf-strides-variant-parquet-prd",
      "${job}",
      "${mergeExisting}",
      "variant"
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
--name "Variant Join ${job} - Studies ${study_ids} - Release ${release_id}" \
--instance-groups "${instance_groups}" \
--scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
--auto-terminate \
--configurations file://./spark-config.json \
--region us-east-1

