#!/bin/zsh

aws emr create-cluster --applications Name=Hadoop Name=Spark \
--ec2-attributes '{"KeyName":"flintrock","AdditionalSlaveSecurityGroups":["sg-059bf5fe80ff903be"],"InstanceProfile":"EMR_EC2_DefaultRole","ServiceAccessSecurityGroup":"sg-0487486fd3d67c14e","SubnetId":"subnet-a756a3ed","EmrManagedSlaveSecurityGroup":"sg-0807b9c40bb37be85","EmrManagedMasterSecurityGroup":"sg-012f30e67b51b6f4d","AdditionalMasterSecurityGroups":["sg-059bf5fe80ff903be"]}' \
--service-role EMR_DefaultRole \
--enable-debugging \
--release-label emr-5.28.0 \
--log-uri 's3n://aws-logs-538745987955-us-east-1/elasticmapreduce/' \
--steps '[{"Args":["spark-submit","--packages","io.projectglow:glow_2.11:0.2.0","--deploy-mode","client","--class","org.kidsfirstdrc.dwh.vcf.ImportVcf","s3a://kf-variant-parquet-prd/jobs/kf-dwh-import-vcf.jar","SD_9PYZAHHE","RE_123456","s3a://kf-study-us-east-1-prd-sd-9pyzahhe/harmonized/family-variants/*.vcf.gz","s3a://kf-variant-parquet-prd", "occurences"],"Type":"CUSTOM_JAR","ActionOnFailure":"TERMINATE_CLUSTER","Jar":"command-runner.jar","Properties":"","Name":"Spark application"}]' \
--name 'Variant Import to DWH - Occurences' \
--instance-groups '[{"InstanceCount":20,"InstanceGroupType":"CORE","InstanceType":"m5d.8xlarge","Name":"Core - 2"},{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":2}]},"InstanceGroupType":"MASTER","InstanceType":"m5.xlarge","Name":"Master - 1"}]' \
--scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
--auto-terminate \
--configurations file://./spark-config.json \
--region us-east-1