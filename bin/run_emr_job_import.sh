#!/bin/bash
study_id=$1
release_id=$2
job=${3:-"all"}
instance_type=${4:-"m5d.4xlarge"}
number_instance=${5:-"20"}
study_id_lc=$(echo "$study_id" | tr '[:upper:]' '[:lower:]')
file_pattern=${6:-"s3a://kf-study-us-east-1-prd-sd-${study_id_lc}/harmonized/family-variants/*postCGP.filtered.deNovo.vep.vcf.gz"}
biospecimen_id_column=${7:-"biospecimen_id"}

steps="[{\"Args\":[\"spark-submit\",\"--packages\",\"io.projectglow:glow_2.11:0.3.0\", \"--exclude-packages\", \"org.apache.httpcomponents:httpcore,org.apache.httpcomponents:httpclient\",\"--deploy-mode\",\"client\",\"--class\",\"org.kidsfirstdrc.dwh.vcf.ImportVcf\",\"s3a://kf-variant-parquet-prd/jobs/kf-dwh-import-vcf.jar\",\"SD_${study_id}\",\"${release_id}\",\"${file_pattern}\",\"s3a://kf-variant-parquet-prd\", \"${job}\", \"${biospecimen_id_column}\"],\"Type\":\"CUSTOM_JAR\",\"ActionOnFailure\":\"TERMINATE_CLUSTER\",\"Jar\":\"command-runner.jar\",\"Properties\":\"\",\"Name\":\"Spark application\"}]"
instance_groups="[{\"InstanceCount\":${number_instance},\"InstanceGroupType\":\"CORE\",\"InstanceType\":\"${instance_type}\",\"Name\":\"Core - 2\"},{\"InstanceCount\":1,\"EbsConfiguration\":{\"EbsBlockDeviceConfigs\":[{\"VolumeSpecification\":{\"SizeInGB\":32,\"VolumeType\":\"gp2\"},\"VolumesPerInstance\":2}]},\"InstanceGroupType\":\"MASTER\",\"InstanceType\":\"m5.xlarge\",\"Name\":\"Master - 1\"}]"

aws emr create-cluster --applications Name=Hadoop Name=Spark \
--ec2-attributes '{"KeyName":"flintrock","AdditionalSlaveSecurityGroups":["sg-059bf5fe80ff903be"],"InstanceProfile":"EMR_EC2_DefaultRole","ServiceAccessSecurityGroup":"sg-0487486fd3d67c14e","SubnetId":"subnet-a756a3ed","EmrManagedSlaveSecurityGroup":"sg-0807b9c40bb37be85","EmrManagedMasterSecurityGroup":"sg-012f30e67b51b6f4d","AdditionalMasterSecurityGroups":["sg-059bf5fe80ff903be"]}' \
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