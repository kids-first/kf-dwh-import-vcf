How to import external databases?
===============

These process requires a lot of manual steps. But it should not be execute a lot of time. Otherwise we should think to automate this.

# Import gnomad

Gnomad provide public Hail tables located into GoogleStorage. Spark/Hadoop need extra configuration to read files from GS : 
In prerequisite, you need a service account in GoogleCloud :
1) Go here https://console.cloud.google.com/apis/credentials?project=localkf
2) Create new service account
3) Doawnload private key information(in json)


1) Start an EMR cluster (1 master - 10 slaves, with Zeppelin, Spark)
```
aws emr create-cluster \
--applications Name=Hadoop Name=Spark Name=Zeppelin Name=JupyterHub \
--ebs-root-volume-size 50 \
--ec2-attributes '{"KeyName":"flintrock","AdditionalSlaveSecurityGroups":["sg-059bf5fe80ff903be"],"InstanceProfile":"EMR_EC2_DefaultRole","ServiceAccessSecurityGroup":"sg-0487486fd3d67c14e","SubnetId":"subnet-a756a3ed","EmrManagedSlaveSecurityGroup":"sg-0807b9c40bb37be85","EmrManagedMasterSecurityGroup":"sg-012f30e67b51b6f4d","AdditionalMasterSecurityGroups":["sg-059bf5fe80ff903be"]}' --service-role EMR_DefaultRole --enable-debugging --release-label emr-5.29.0 --log-uri 's3n://aws-logs-538745987955-us-east-1/elasticmapreduce/' \
--name 'Import external databases' \
--instance-groups '[{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":300,"VolumeType":"gp2"},"VolumesPerInstance":1}],"EbsOptimized":true},"InstanceGroupType":"MASTER","InstanceType":"m5.xlarge","Name":"Master - 1"},{"InstanceCount":10,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":64,"VolumeType":"gp2"},"VolumesPerInstance":4}]},"InstanceGroupType":"CORE","InstanceType":"m5.4xlarge","Name":"Core - 2"}]' \
--configurations '[{"Classification":"spark","Properties":{"maximizeResourceAllocation":"true"}},{"Classification":"spark-defaults","Properties":{"spark.jars.packages":"com.amazonaws:aws-java-sdk:1.11.683,org.apache.hadoop:hadoop-aws:2.8.5,io.projectglow:glow_2.11:0.2.0","spark.hadoop.fs.s3a.connection.maximum":"5000","spark.pyspark.python":"/usr/bin/python3"}},{"Classification":"emrfs-site","Properties":{"fs.s3.maxConnections":"5000"}},{"Classification":"spark-hive-site","Properties":{"hive.metastore.client.factory.class":"com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"}}]' \
--scale-down-behavior TERMINATE_AT_TASK_COMPLETION --region us-east-1
```
2) connect on the master with ssh
3) Download this [jar](https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop2-latest.jar) and copy this into /usr/lib/spark/jars
4) Add into /etc/hadoop/conf/core-site.xml :
```
<property>
  <name>fs.AbstractFileSystem.gs.impl</name>
  <value>com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS</value>
  <description>The AbstractFileSystem for gs: uris.</description>
</property>
  <property>
    <name>fs.gs.auth.service.account.private.key.id</name>
    <value>my_private_key_id</value>
    <description>
      The private key id associated with the service account used for GCS access.
      This can be extracted from the json keyfile generated via the Google Cloud
      Console.
    </description>
  </property>
  <property>
    <name>fs.gs.auth.service.account.private.key</name>
   <value>
-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDZ3lsC/AcSjja9
      Console.
    </description>
  </property>
  <property>
    <name>fs.gs.auth.service.account.private.key</name>
   <value>
-----BEGIN PRIVATE KEY-----
firstline
secondline
.....
-----END PRIVATE KEY-----
</value>
   <description>
      The private key associated with the service account used for GCS access.
      This can be extracted from the json keyfile generated via the Google Cloud
      Console.
    </description>
  </property>
  <property>
    <name>fs.gs.auth.service.account.email</name>
    <value>my@example.iam.gserviceaccount.com</value>
    <description>
      The email address is associated with the service account used for GCS
      access when fs.gs.auth.service.account.enable is true. Required
      when authentication key specified in the Configuration file (Method 1)
      or a PKCS12 certificate (Method 3) is being used.
    </description>
  </property>
``` 
The value of the fields can be found in json file associated to the service account.

You need to download hail and configure spark :
sudo pip-3.6 install wheel
sudo pip-3.6 install hail

Then, locate hail directrory (HAIL_DIR) with :
sudo pip-3.6 show hail


Edit /etc/spark/conf/ :
1) Add spark.jars
2) append spark.driver.extraClassPath and spark.executor.extraClassPath
```
spark.jars                       /usr/local/lib/python3.6/site-packages/hail/backend/hail-all-spark.jar
spark.driver.extraClassPath      .....:/usr/share/aws/emr/s3select/lib/emr-s3-select-spark-connector.jar:/usr/local/lib/python3.6/site-packages/hail/backend/hail-all-spark.jar
spark.executor.extraClassPath    .....:/usr/local/lib/python3.6/site-packages/hail/backend/hail-all-spark.jar
spark.serializer          org.apache.spark.serializer.KryoSerializer
spark.kryo.registrator          is.hail.kryo.HailKryoRegistrator
```

Restart zeppelin context
In a new notebook, copy the content of the file gnomad.py and run it


Import DBNSFP :
1) In a new notebook copy the lines that are ine comment in file schema.scala.
2) Copy also the content of ImportDBSNFP (starting at line spark.read...)
3) Run it 


Import Topmed, 1000Genomes, etc...
1) In a new notebook copy the content of the files (starting at spark.read...)
2) Run it


Import clinvar
1) Upload `clinvar_ingestion.sh` to s3
   ```shell
   aws s3 cp bin/clinvar_ingestion.sh s3://kf-strides-variant-parquet-prd/jobs/bootstrap-actions/clinvar_ingestion.sh
   ```
2) Build and upload `kf-dwh-import.jar` to s3
   ```shell
   sbt assembly
   aws s3 cp target/scala-2.12/kf-dwh-import-vcf-$VERSION.jar s3://kf-strides-variant-parquet-prd/jobs/kf-dwh-import-vcf-$VERSION.jar
   ```
3) run `bin/run_emr_job_import_clinvar.sh`

   ```shell
   cd bin
   ./run_emr_job_import_External.sh
   ```



