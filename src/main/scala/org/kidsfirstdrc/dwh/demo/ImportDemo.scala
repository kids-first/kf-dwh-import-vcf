package org.kidsfirstdrc.dwh.demo

import bio.ferlab.datalake.core.config.{Configuration, StorageConf}
import org.apache.spark.sql.SparkSession
import org.kidsfirstdrc.dwh.vcf.Variants

object ImportDemo extends App {

  val Array(releaseId, input, output, runType) = args

  implicit val spark: SparkSession = SparkSession.builder
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .appName(s"Import $runType for $demoStudyId - $releaseId").getOrCreate()

  implicit val conf: Configuration = Configuration(List(
    StorageConf("kf-strides-variant", "s3a://kf-strides-variant-parquet-prd/public/demo")
  ))

  run(demoStudyId, releaseId, input, output, runType)

  def run(studyId: String, releaseId: String, input: String, output: String, runType: String = "all")(implicit spark: SparkSession): Unit = {
    spark.sql("use demo")
    runType match {
      case "occurrences" => DemoOccurrences.run(studyId, releaseId, input)
      case "variants" => new Variants(studyId, releaseId).run()
      case "consequences" => DemoConsequences.run(studyId, releaseId, input, output)
      case "all" =>
        DemoOccurrences.run(studyId, releaseId, input)
        new Variants(studyId, releaseId).run()
        DemoConsequences.run(studyId, releaseId, input, output)

    }
  }
}

