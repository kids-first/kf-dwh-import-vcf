#!/bin/bash

# kf-strides-apps-prd-us-east-1-vpc-private-us-east-1a
# 0f0c909ec60b377ce
aws s3 cp bootstrap-actions s3://kf-strides-variant-parquet-prd/jobs/bootstrap-actions --recursive

aws emr create-cluster --applications Name=Hadoop Name=Spark Name=Zeppelin Name=JupyterHub \
  --ebs-root-volume-size 50 \
  --ec2-attributes '{"KeyName":"flintrock","InstanceProfile":"kf-variant-emr-ec2-prd-profile","SubnetId":"subnet-00aab84919d5a44e2","ServiceAccessSecurityGroup":"sg-0587a1d20e24f4104","EmrManagedSlaveSecurityGroup":"sg-0dc6b48e674070821","EmrManagedMasterSecurityGroup":"sg-0a31895d33d1643da"}' \
  --service-role kf-variant-emr-prd-role \
  --enable-debugging \
  --release-label emr-6.3.0 \
  --log-uri 's3n://kf-strides-variant-parquet-prd/jobs/elasticmapreduce/' \
  --bootstrap-actions Path="s3://kf-strides-variant-parquet-prd/jobs/bootstrap-actions/enable-ssm.sh" Path="s3://kf-strides-variant-parquet-prd/jobs/bootstrap-actions/download_human_reference_genome.sh" Path="s3://kf-strides-variant-parquet-prd/jobs/bootstrap-actions/setup-delta-lake.sh"\
  --name 'Variant with Glow' \
  --instance-groups '[{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":300,"VolumeType":"gp2"},"VolumesPerInstance":1}],"EbsOptimized":true},"InstanceGroupType":"MASTER","InstanceType":"m5.4xlarge","Name":"Master - 1"},{"InstanceCount":3,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":500,"VolumeType":"gp2"},"VolumesPerInstance":4}]},"InstanceGroupType":"CORE","InstanceType":"m5.4xlarge","Name":"Core - 2"}]' \
  --configurations '[{"Classification":"spark","Properties":{"maximizeResourceAllocation":"true"}},{"Classification":"spark-defaults","Properties":{"spark.jars.packages":"bio.ferlab:datalake-spark3_2.12:0.0.51","spark.hadoop.fs.s3a.connection.maximum":"5000","spark.hadoop.fs.s3a.aws.credentials.provider":"org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider", "spark.hadoop.fs.s3a.bucket.kf-strides-variant-parquet-prd.aws.credentials.provider":"com.amazonaws.auth.InstanceProfileCredentialsProvider","spark.hadoop.fs.s3a.assumed.role.arn":"arn:aws:iam::538745987955:role/kf-etl-server-prd-role","spark.hadoop.fs.s3a.assumed.role.credentials.provider":"com.amazonaws.auth.InstanceProfileCredentialsProvider","spark.pyspark.python":"/usr/bin/python3"}},{"Classification":"emrfs-site","Properties":{"fs.s3.maxConnections":"5000"}},{"Classification":"spark-hive-site","Properties":{"hive.metastore.client.factory.class":"com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"}}]' \
  --scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
  --region us-east-1
