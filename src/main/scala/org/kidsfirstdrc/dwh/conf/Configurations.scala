package org.kidsfirstdrc.dwh.conf

import bio.ferlab.datalake.commons.config.{Configuration, ConfigurationWriter, StorageConf}
import bio.ferlab.datalake.commons.file.FileSystemType.S3

object Configurations extends App {

  //example of storages per environment
  val productionStorage = List(StorageConf(Catalog.kfStridesVariantBucket, "s3a://kf-strides-variant-parquet-prd", S3))
  val qaStorage = List(StorageConf(Catalog.kfStridesVariantBucket, "s3a://kf-strides-variant-parquet-prd", S3))
  val localStorage = List(StorageConf(Catalog.kfStridesVariantBucket, getClass.getClassLoader.getResource(".").getFile, S3))

  // common options for EMR execution
  val emrOptions = Map("hive.metastore.client.factory.class" -> "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")

  //example of configurations per env
  val productionConfiguration = Configuration(productionStorage, Catalog.sources.toList, List(), emrOptions)
  val qaConfiguration = Configuration(qaStorage, Catalog.sources.toList, List(), emrOptions)
  val localConfiguration = Configuration(List(), Catalog.sources.toList, List(), Map())

  // example of output for each file
  ConfigurationWriter.writeTo("src/main/resources/config/prd.conf", productionConfiguration)
  ConfigurationWriter.writeTo("src/main/resources/config/qa.conf", qaConfiguration)
  ConfigurationWriter.writeTo("src/test/resources/config/test.conf", localConfiguration)

}
