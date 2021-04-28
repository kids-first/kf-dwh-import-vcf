#!/bin/bash
release_id=$1
job=${2:-"all"}
instance_type=${3:-"m5d.4xlarge"}
number_instance=${4:-"20"}
file_pattern=${5:-"s3a://bix-dev-data-bucket/PROJECTS/rs-vpf5jbc3-cov-irt-controlled-access-study/VEP_VCFS/*.vep.vcf.gz"}

steps="[{\"Args\":[\"spark-submit\",\"--packages\",\"io.projectglow:glow_2.11:0.3.0\", \"--exclude-packages\", \"org.apache.httpcomponents:httpcore,org.apache.httpcomponents:httpclient\",\"--deploy-mode\",\"client\",\"--class\",\"org.kidsfirstdrc.dwh.covirt.ImportCovirt\",\"s3a://kf-strides-variant-parquet-prd/jobs/kf-dwh-import-vcf.jar\",\"${release_id}\",\"${file_pattern}\",\"s3a://kf-strides-variant-parquet-prd\", \"${job}\"],\"Type\":\"CUSTOM_JAR\",\"ActionOnFailure\":\"TERMINATE_CLUSTER\",\"Jar\":\"command-runner.jar\",\"Properties\":\"\",\"Name\":\"Spark application\"}]"
instance_groups="[{\"InstanceCount\":${number_instance},\"InstanceGroupType\":\"CORE\",\"InstanceType\":\"${instance_type}\",\"Name\":\"Core - 2\"},{\"InstanceCount\":1,\"EbsConfiguration\":{\"EbsBlockDeviceConfigs\":[{\"VolumeSpecification\":{\"SizeInGB\":32,\"VolumeType\":\"gp2\"},\"VolumesPerInstance\":2}]},\"InstanceGroupType\":\"MASTER\",\"InstanceType\":\"m5.xlarge\",\"Name\":\"Master - 1\"}]"

aws emr create-cluster --applications Name=Hadoop Name=Spark \
--ec2-attributes '{"KeyName":"flintrock","AdditionalSlaveSecurityGroups":["sg-059bf5fe80ff903be"],"InstanceProfile":"EMR_EC2_DefaultRole","ServiceAccessSecurityGroup":"sg-0487486fd3d67c14e","SubnetId":"subnet-a756a3ed","EmrManagedSlaveSecurityGroup":"sg-0807b9c40bb37be85","EmrManagedMasterSecurityGroup":"sg-012f30e67b51b6f4d","AdditionalMasterSecurityGroups":["sg-059bf5fe80ff903be"]}' \
--service-role EMR_DefaultRole \
--enable-debugging \
--release-label emr-5.32.0 \
--log-uri 's3n://aws-logs-538745987955-us-east-1/elasticmapreduce/' \
--steps "${steps}" \
--name "Covirt Import - ${job} - Release ${release_id}" \
--instance-groups "${instance_groups}" \
--scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
--auto-terminate \
--configurations file://./spark-config.json \
--region us-east-1