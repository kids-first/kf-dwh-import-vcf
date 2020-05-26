package org.kidsfirstdrc.dwh.join

import org.apache.spark.sql.SparkSession

object Join extends App {
  val Array(studyId, releaseId, output, runType, mergeExisting) = args
  implicit val spark: SparkSession = SparkSession.builder
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .appName(s"Join $runType for $studyId - $releaseId").getOrCreate()

  run(studyId, releaseId, output, runType, mergeExisting.toBoolean)

  def run(studyId: String, releaseId: String, output: String, runType: String, mergeExisting: Boolean)(implicit spark: SparkSession): Unit = {
    val studyIds = studyId.split(",")
    val releaseIdLc = releaseId.toLowerCase()

    spark.sql("use variant")
    if (runType == "all") {
      JoinVariants.join(studyIds, releaseIdLc, output, mergeExisting)
      JoinConsequences.join(studyIds, releaseIdLc, output, mergeExisting)
    }
    else if (runType == "variants")
      JoinVariants.join(studyIds, releaseIdLc, output, mergeExisting)
    else if (runType == "consequences")
      JoinConsequences.join(studyIds, releaseIdLc, output, mergeExisting)
  }

}
