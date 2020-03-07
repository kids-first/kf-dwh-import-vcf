package org.kidsfirstdrc.dwh.vcf

import org.apache.spark.sql.SparkSession

object ImportVcf extends App {

  val Array(studyId, releaseId, input, output, runType) = args

  implicit val spark: SparkSession = SparkSession.builder
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .appName(s"Import $runType for $studyId - $releaseId").getOrCreate()

  run(studyId, releaseId, input, output, runType)

  def run(studyId: String, releaseId: String, input: String, output: String, runType: String = "all")(implicit spark: SparkSession): Unit = {
    spark.sql("use variant")
    if (runType == "all") {
      Occurences.run(studyId, releaseId, input, output)
      Variants.run(studyId, releaseId, input, output)
      Consequences.run(studyId, releaseId, input, output)
    }
    else if (runType == "occurences")
      Occurences.run(studyId, releaseId, input, output)
    else if (runType == "annotations")
      Variants.run(studyId, releaseId, input, output)
    else if (runType == "consequences")
      Consequences.run(studyId, releaseId, input, output)
  }


}

