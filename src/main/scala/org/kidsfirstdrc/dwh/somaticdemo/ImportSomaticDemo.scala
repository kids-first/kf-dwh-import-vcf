package org.kidsfirstdrc.dwh.somaticdemo

import org.apache.spark.sql.SparkSession

object ImportSomaticDemo extends App {

  val Array(studyId, releaseId, input, output, runType) = args

  implicit val spark: SparkSession = SparkSession.builder
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .appName(s"Import $runType for $studyId - $releaseId").getOrCreate()

  run(studyId, releaseId, input, output, runType)

  def run(studyId: String, releaseId: String, input: String, output: String, runType: String = "all")(implicit spark: SparkSession): Unit = {
    spark.sql("use variant")
    if (runType == "all") {
      Occurrences.run(studyId, releaseId, input, output)
      Variants.run(studyId, releaseId, input, output)
      Consequences.run(studyId, releaseId, input, output)
    }
    else if (runType == "occurrences")
      Occurrences.run(studyId, releaseId, input, output)
    else if (runType == "variants")
      Variants.run(studyId, releaseId, input, output)
    else if (runType == "consequences")
      Consequences.run(studyId, releaseId, input, output)
  }


}

