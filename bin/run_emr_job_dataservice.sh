#!/bin/bash
study_ids=$1
release_id=$2
mergeExisting=${3:-"true"}
tables=${4:-"all"}
instance_type=${5:-"m5.xlarge"}

steps=$(cat <<EOF
[
  {
    "Args": [
      "spark-submit",
      "--deploy-mode",
      "client",
      "--class",
      "org.kidsfirstdrc.dwh.external.ImportDataservice",
      "s3a://kf-variant-parquet-prd/jobs/kf-dwh-import-vcf.jar",
      "${study_ids}",
      "${release_id}",
      "s3a://kf-variant-parquet-prd/raw/dataservice",
      "s3a://kf-variant-parquet-prd",
      "${mergeExisting}",
      "${tables}"
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
instance_groups="[{\"InstanceCount\":1,\"InstanceGroupType\":\"CORE\",\"InstanceType\":\"${instance_type}\",\"Name\":\"Core - 2\"},{\"InstanceCount\":1,\"EbsConfiguration\":{\"EbsBlockDeviceConfigs\":[{\"VolumeSpecification\":{\"SizeInGB\":32,\"VolumeType\":\"gp2\"},\"VolumesPerInstance\":2}]},\"InstanceGroupType\":\"MASTER\",\"InstanceType\":\"m5.xlarge\",\"Name\":\"Master - 1\"}]"

aws emr create-cluster --applications Name=Hadoop Name=Spark \
--ec2-attributes '{"KeyName":"flintrock","AdditionalSlaveSecurityGroups":["sg-059bf5fe80ff903be"],"InstanceProfile":"EMR_EC2_DefaultRole","ServiceAccessSecurityGroup":"sg-0487486fd3d67c14e","SubnetId":"subnet-a756a3ed","EmrManagedSlaveSecurityGroup":"sg-0807b9c40bb37be85","EmrManagedMasterSecurityGroup":"sg-012f30e67b51b6f4d","AdditionalMasterSecurityGroups":["sg-059bf5fe80ff903be"]}' \
--service-role EMR_DefaultRole \
--enable-debugging \
--release-label emr-5.28.0 \
--log-uri 's3n://aws-logs-538745987955-us-east-1/elasticmapreduce/' \
--steps "${steps}" \
--name "Import dataservice tables ${tables} - Studies ${study_ids} - Release ${release_id}" \
--instance-groups "${instance_groups}" \
--scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
--auto-terminate \
--configurations file://./spark-config.json \
--region us-east-1