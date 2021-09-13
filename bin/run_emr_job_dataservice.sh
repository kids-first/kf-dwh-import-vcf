#!/bin/bash
study_ids=${1:-"SD_46SK55A3,SD_9PYZAHHE,SD_DYPMEHHF,SD_BHJXBDQK,SD_7NQ9151J,SD_NMVV8A1Y"} # "SD_DZTB5HRR"}
release_id=${2:-"RE_000012"}
merge_existing=${3:-"false"}
tables=${4:-"all"}
instance_type=${5:-"m5.4xlarge"}
instance_count=${6:-"5"}
env=${7:-"qa"}

# SD_46SK55A3,SD_9PYZAHHE,SD_DYPMEHHF,SD_BHJXBDQK,SD_7NQ9151J,SD_NMVV8A1Y
# SD_DZTB5HRR,SD_ZXJFFMEF,SD_6FPYJQBR,SD_YGVA0E1C,
# SD_RM8AFW0R,SD_W0V965XZ,SD_R0EPRSGS,SD_P445ACHV,SD_VTTSHWV4,SD_JWS3V24D,SD_0TYVY1TW,SD_DZ4GPQX6,SD_PREASA7S,SD_46RR9ZR6,
# SD_DK0KRWK8,SD_B8X3C1MX # NOT allowed


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
      "org.kidsfirstdrc.dwh.external.ImportDataservice",
      "s3a://kf-strides-variant-parquet-prd/jobs/${env}/kf-dwh-import-vcf.jar",
      "${study_ids}",
      "${release_id}",
      "s3a://kf-strides-variant-parquet-prd/raw/dataservice",
      "s3a://kf-strides-variant-parquet-prd/dataservice",
      "${merge_existing}",
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
instance_groups="[{\"InstanceCount\":${instance_count},\"InstanceGroupType\":\"CORE\",\"InstanceType\":\"${instance_type}\",\"Name\":\"Core - 2\"},{\"InstanceCount\":1,\"EbsConfiguration\":{\"EbsBlockDeviceConfigs\":[{\"VolumeSpecification\":{\"SizeInGB\":32,\"VolumeType\":\"gp2\"},\"VolumesPerInstance\":2}]},\"InstanceGroupType\":\"MASTER\",\"InstanceType\":\"m5.4xlarge\",\"Name\":\"Master - 1\"}]"

aws emr create-cluster --applications Name=Hadoop Name=Spark \
--ec2-attributes '{"KeyName":"flintrock","InstanceProfile":"kf-variant-emr-ec2-prd-profile","ServiceAccessSecurityGroup":"sg-0587a1d20e24f4104","SubnetId":"subnet-00aab84919d5a44e2","EmrManagedSlaveSecurityGroup":"sg-0dc6b48e674070821","EmrManagedMasterSecurityGroup":"sg-0a31895d33d1643da"}' \
--service-role kf-variant-emr-prd-role \
--enable-debugging \
--release-label emr-6.3.0 \
--log-uri 's3n://kf-strides-variant-parquet-prd/jobs/elasticmapreduce/' \
--steps "${steps}" \
--name "Import dataservice tables ${tables} - Studies ${study_ids} - Release ${release_id}" \
--instance-groups "${instance_groups}" \
--scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
--auto-terminate \
--configurations file://./spark-config.json \
--region us-east-1
