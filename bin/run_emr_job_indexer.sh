#!/bin/bash
release_id=${1:-"re_000012"}
input=${2:-"s3a://kf-strides-variant-parquet-prd/portal/es_index/gene_centric/"}
es_nodes=${3:-"https://vpc-kf-arranger-blue-es-service-exwupkrf4dyupg24dnfmvzcwri.us-east-1.es.amazonaws.com:443"}
#es_nodes=${3:-"https://vpc-kf-arranger-blue-es-prd-4gbc2zkvm5uttysiqkcbzwxqeu.us-east-1.es.amazonaws.com:443"}
es_index_name=${4:-"gene_centric"}
es_index_template=${5:-"gene_centric_template.json"}
es_job_type=${6:-"index"} # one of: index, update, upsert or create
es_batch_size=${7:-"500"} #default is 1000
chromosome=${8:-"all"} #all, 1, 2, 3, ..., X, Y
jarV=${9:-"7.12.0"}
number_instance=${10:-"1"}
instance_type=${11:-"m5.2xlarge"}
env=${12:-"qa"}
format=${13:-"parquet"}
repartition=${14:-"10000"}
previous_release_id=${15:-"re_000011"}
aws s3 cp templates s3://kf-strides-variant-parquet-prd/jobs/templates --recursive

# default is dev vpc-05be68d35774905e8
subnetId="subnet-0f822f9f9ff99871a"
serviceAccessSecurityGroup="sg-04894e9def6241eba"
emrManagedSlaveSecurityGroup="sg-0c131e9d64cec6a14"
emrManagedMasterSecurityGroup="sg-01a0dfc74131cff1d"
emrServiceRole=kf-variant-emr-prd-role # EMR_DefaultRole
ec2ProfileRole=kf-variant-emr-ec2-prd-profile #EMR_EC2_DefaultRole

#if env = prod
if [ ${env} == "prod" ]; then subnetId="subnet-00aab84919d5a44e2"; fi
if [ ${env} == "prod" ]; then serviceAccessSecurityGroup="sg-0587a1d20e24f4104"; fi
if [ ${env} == "prod" ]; then emrManagedSlaveSecurityGroup="sg-0dc6b48e674070821"; fi
if [ ${env} == "prod" ]; then emrManagedMasterSecurityGroup="sg-0a31895d33d1643da"; fi

steps=$(cat <<EOF
[
  {
    "Args": [
      "spark-submit",
      "--deploy-mode", "client",
      "--packages", "org.elasticsearch:elasticsearch-spark-30_2.12:${jarV},commons-httpclient:commons-httpclient:3.1",
      "--class", "org.kidsfirstdrc.dwh.es.index.Indexer",
      "s3a://kf-strides-variant-parquet-prd/jobs/${env}/kf-dwh-import-vcf.jar",
      "${input}",
      "${es_nodes}",
      "${es_index_name}",
      "${previous_release_id}",
      "${release_id}",
      "${es_index_template}",
      "${es_job_type}",
      "${es_batch_size}",
      "${chromosome}",
      "${format}",
      "${repartition}"
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
ec2_attributes="{\"KeyName\":\"flintrock\",\"InstanceProfile\":\"${ec2ProfileRole}\",\"ServiceAccessSecurityGroup\":\"${serviceAccessSecurityGroup}\",\"SubnetId\":\"${subnetId}\",\"EmrManagedSlaveSecurityGroup\":\"${emrManagedSlaveSecurityGroup}\",\"EmrManagedMasterSecurityGroup\":\"${emrManagedMasterSecurityGroup}\"}"

aws emr create-cluster --applications Name=Hadoop Name=Spark \
--ec2-attributes "${ec2_attributes}" \
--service-role ${emrServiceRole} \
--enable-debugging \
--release-label emr-6.3.0 \
--log-uri 's3n://kf-strides-variant-parquet-prd/jobs/elasticmapreduce/' \
--steps "${steps}" \
--name "${es_index_name}_${chromosome} index to ES7 ${release_id} - ${env}" \
--instance-groups "${instance_groups}" \
--scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
--auto-terminate \
--configurations file://./spark-config.json \
--region us-east-1

