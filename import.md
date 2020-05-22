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


Import Topmed, 1000Genomes, etc....
1) In a new notebook copy the content of the files (starting at spark.read...)
2) Run it



