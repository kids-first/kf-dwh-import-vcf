#!/bin/bash
study_id=$1
release_id=$2
input_vcf=${3:-"s3a://kf-study-us-east-1-prd-${study_id_lc}/harmonized/family-variants"}
job=${4:-"all"}
instance_type=${5:-"m5d.8xlarge"}
number_instance=${6:-"30"}
study_id_lc=$(echo "$study_id" | tr '[:upper:]' '[:lower:]' | tr '_' '-')
biospecimen_id_column=${7:-"biospecimen_id"}
steps=$(cat <<EOF
[
  {
    "Args": [
      "spark-submit",
      "--packages",
      "io.projectglow:glow_2.11:0.5.0",
      "--exclude-packages",
      "org.apache.httpcomponents:httpcore,org.apache.httpcomponents:httpclient",
      "--deploy-mode",
      "client",
      "--class",
      "org.kidsfirstdrc.dwh.vcf.ImportVcf",
      "s3a://kf-strides-variant-parquet-prd/jobs/kf-dwh-import-vcf.jar"
      "${study_id}",
      "${release_id}",
      "${input_vcf}",
      "s3a://kf-strides-variant-parquet-prd",
      "${job}",
      "${biospecimen_id_column}"
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

instance_groups="[{\"InstanceCount\":${number_instance},\"InstanceGroupType\":\"CORE\",\"InstanceType\":\"${instance_type}\",\"Name\":\"Core - 2\"},{\"InstanceCount\":1,\"EbsConfiguration\":{\"EbsBlockDeviceConfigs\":[{\"VolumeSpecification\":{\"SizeInGB\":32,\"VolumeType\":\"gp2\"},\"VolumesPerInstance\":2}]},\"InstanceGroupType\":\"MASTER\",\"InstanceType\":\"m5.xlarge\",\"Name\":\"Master - 1\"}]"

aws emr create-cluster --applications Name=Hadoop Name=Spark \
--ec2-attributes '{"KeyName":"flintrock","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-031b7ef17a032fc3b","EmrManagedSlaveSecurityGroup":"sg-0d04e7c3ff5f36538","EmrManagedMasterSecurityGroup":"sg-0abad24e2a3e5e279"}' \
--service-role EMR_DefaultRole \
--enable-debugging \
--release-label emr-5.29.0 \
--log-uri 's3n://aws-logs-538745987955-us-east-1/elasticmapreduce/' \
--steps "${steps}" \
--name "Variant Import - ${job} - Study ${study_id} - Release ${release_id}" \
--instance-groups "${instance_groups}" \
--scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
--auto-terminate \
--configurations file://./spark-config.json \
--region us-east-1
