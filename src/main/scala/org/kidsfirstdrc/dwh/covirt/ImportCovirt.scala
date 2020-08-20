package org.kidsfirstdrc.dwh.covirt

import org.apache.spark.sql.SparkSession

object ImportCovirt extends App {

  val Array(releaseId, input, output, runType) = args

  implicit val spark: SparkSession = SparkSession.builder
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .appName(s"Import $runType for covirt - $releaseId").getOrCreate()

  run(releaseId, input, output, runType)

  def run(releaseId: String, input: String, output: String, runType: String = "all")(implicit spark: SparkSession): Unit = {
    spark.sql("use covirt")
    if (runType == "all") {
      CovirtOccurrences.run(releaseId, input, output)
//      Variants.run(studyId, releaseId, input, output)
      Consequences.run(releaseId, input, output)
    }
    else if (runType == "occurrences")
      CovirtOccurrences.run(releaseId, input, output)
//    else if (runType == "annotations")
//      Variants.run(studyId, releaseId, input, output)
    else if (runType == "consequences")
      Consequences.run(releaseId, input, output)
  }


}

