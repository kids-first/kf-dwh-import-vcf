#!/bin/bash
set -x
study_id=$1
release_id=${2:-"re_000011"}
study_id_lc=$(echo "$study_id" | tr '[:upper:]' '[:lower:]' | tr '_' '-')
job=${3:-"occurrences"}
schema=${4:-"portal"}
cgp_pattern=${5:-".CGP.filtered.deNovo.vep.vcf.gz"}
post_cgp_pattern=${6:-".postCGP.filtered.deNovo.vep.vcf.gz"}
input_vcf=${7:-"s3a://kf-study-us-east-1-prd-${study_id_lc}/harmonized/family-variants/"}
#input_vcf=${9:-"s3a://kf-study-us-east-1-prd-${study_id_lc}/harmonized-data/simple-variants/"}
#post_cgp_pattern=${7:-"CGP.filtered.vep.vcf.gz"}
biospecimen_id_column=${8:-"biospecimen_id"}
instance_count=${9:-"15"}
instance_type=${10:-"m5.4xlarge"}


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
      "--class",
      "org.kidsfirstdrc.dwh.vcf.ImportVcf",
      "s3a://kf-strides-variant-parquet-prd/jobs/kf-dwh-import-vcf.jar",
      "${study_id}",
      "${release_id}",
      "${input_vcf}",
      "${job}",
      "${biospecimen_id_column}",
      "${cgp_pattern}",
      "${post_cgp_pattern}",
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

instance_groups="[{\"InstanceCount\":${instance_count},\"InstanceGroupType\":\"CORE\",\"InstanceType\":\"${instance_type}\",\"Name\":\"Core - 2\"},{\"InstanceCount\":1,\"EbsConfiguration\":{\"EbsBlockDeviceConfigs\":[{\"VolumeSpecification\":{\"SizeInGB\":32,\"VolumeType\":\"gp2\"},\"VolumesPerInstance\":2}]},\"InstanceGroupType\":\"MASTER\",\"InstanceType\":\"m5.xlarge\",\"Name\":\"Master - 1\"}]"

aws emr create-cluster --applications Name=Hadoop Name=Spark \
--ec2-attributes '{"KeyName":"flintrock","InstanceProfile":"kf-variant-emr-ec2-prd-profile","SubnetId":"subnet-031b7ef17a032fc3b","EmrManagedSlaveSecurityGroup":"sg-0d04e7c3ff5f36538","EmrManagedMasterSecurityGroup":"sg-0abad24e2a3e5e279"}' \
--service-role kf-variant-emr-prd-role \
--enable-debugging \
--release-label emr-6.2.0 \
--log-uri 's3n://kf-strides-variant-parquet-prd/jobs/elasticmapreduce/' \
--steps "${steps}" \
--name "Import - ${job} - ${study_id} - ${release_id} - ${schema}" \
--instance-groups "${instance_groups}" \
--scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
--auto-terminate \
--configurations file://./spark-config.json \
--region us-east-1
