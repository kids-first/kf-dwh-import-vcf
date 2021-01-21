package org.kidsfirstdrc.dwh.demo

import org.apache.spark.sql.SparkSession
import org.kidsfirstdrc.dwh.vcf.Variants

object ImportDemo extends App {

  val Array(releaseId, input, output, runType) = args

  implicit val spark: SparkSession = SparkSession.builder
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .appName(s"Import $runType for $demoStudyId - $releaseId").getOrCreate()

  run(demoStudyId, releaseId, input, output, runType)

  def run(studyId: String, releaseId: String, input: String, output: String, runType: String = "all")(implicit spark: SparkSession): Unit = {
    spark.sql("use demo")
    runType match {
      case "occurrences" => DemoOccurrences.run(studyId, releaseId, input, output)
      case "variants" => Variants.run(studyId, releaseId, input, output)
      case "consequences" => DemoConsequences.run(studyId, releaseId, input, output)
      case "all" =>
        DemoOccurrences.run(studyId, releaseId, input, output)
        Variants.run(studyId, releaseId, input, output)
        DemoConsequences.run(studyId, releaseId, input, output)

    }
  }
}

