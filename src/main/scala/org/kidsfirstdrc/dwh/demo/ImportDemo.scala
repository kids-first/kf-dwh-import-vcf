package org.kidsfirstdrc.dwh.demo

import bio.ferlab.datalake.spark3.config.{Configuration, StorageConf}
import org.apache.spark.sql.SparkSession
import org.kidsfirstdrc.dwh.vcf.Variants

object ImportDemo extends App {

  val Array(releaseId, input, runType) = args

  implicit val spark: SparkSession = SparkSession.builder
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .appName(s"Import $runType for $demoStudyId - $releaseId").getOrCreate()

  implicit val conf: Configuration = Configuration(List(
    StorageConf("kf-strides-variant", "s3a://kf-strides-variant-parquet-prd/public/demo")
  ))

  run(demoStudyId, releaseId, input, runType)

  def run(studyId: String, releaseId: String, input: String, runType: String = "all")(implicit spark: SparkSession): Unit = {
    spark.sql("use demo")
    runType match {
      case "occurrences" => new DemoOccurrences(studyId, releaseId, input).run()
      case "variants" => new Variants(studyId, releaseId, "demo").run()
      case "consequences" => new DemoConsequences(studyId, releaseId, input).run()
      case "all" =>
        new DemoOccurrences(studyId, releaseId, input).run()
        new Variants(studyId, releaseId, "demo").run()
        new DemoConsequences(studyId, releaseId, input).run()

    }
  }
}

