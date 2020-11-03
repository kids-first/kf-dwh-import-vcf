#!/bin/bash
es_host=$1
instance_type=${2:-"m5d.4xlarge"}

steps=$(cat <<EOF
[
  {
    "Args": [
      "spark-submit",
      "--deploy-mode",
      "client",
      "--class",
      "org.kidsfirstdrc.dwh.variantDbStats.Stats",
      "s3a://kf-variant-parquet-prd/jobs/kf-dwh-import-vcf.jar",
      "${es_host}"
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

instance_groups="[{\"InstanceCount\":30,\"InstanceGroupType\":\"CORE\",\"InstanceType\":\"${instance_type}\",\"Name\":\"Core - 2\"},{\"InstanceCount\":1,\"EbsConfiguration\":{\"EbsBlockDeviceConfigs\":[{\"VolumeSpecification\":{\"SizeInGB\":32,\"VolumeType\":\"gp2\"},\"VolumesPerInstance\":2}]},\"InstanceGroupType\":\"MASTER\",\"InstanceType\":\"m5.xlarge\",\"Name\":\"Master - 1\"}]"

aws emr create-cluster --applications Name=Hadoop Name=Spark \
--ec2-attributes '{"KeyName":"flintrock","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-031b7ef17a032fc3b","EmrManagedSlaveSecurityGroup":"sg-0d04e7c3ff5f36538","EmrManagedMasterSecurityGroup":"sg-0abad24e2a3e5e279"}' \
--service-role EMR_DefaultRole \
--enable-debugging \
--release-label emr-5.28.0 \
--log-uri 's3n://kf-strides-variant-parquet-prd/jobs/elasticmapreduce/' \
--steps "${steps}" \
--name "Index Variant Stats" \
--instance-groups "${instance_groups}" \
--scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
--auto-terminate \
--configurations file://./spark-config.json \
--region us-east-1