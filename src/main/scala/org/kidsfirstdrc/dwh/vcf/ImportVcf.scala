package org.kidsfirstdrc.dwh.vcf

import org.apache.spark.sql.SparkSession

import scala.util.Try

object ImportVcf extends App {

  val Array(studyId, releaseId, input, output, runType, biospecimenIdColumn, isPostCGPOnlyStr) = args

  implicit val spark: SparkSession = SparkSession.builder
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .appName(s"Import $runType for $studyId - $releaseId").getOrCreate()

  val isPostCGPOnly: Boolean = Try(isPostCGPOnlyStr.toBoolean).getOrElse(false)

  run(studyId, releaseId, input, output, runType, biospecimenIdColumn, isPostCGPOnly)

  def run(studyId: String, releaseId: String, input: String, output: String,
          runType: String = "all", biospecimenIdColumn: String = "biospecimen_id", isPostCGPOnly: Boolean = false)
         (implicit spark: SparkSession): Unit = {
    spark.sql("use variant")
    if (runType == "all") {
      Occurrences.run(studyId, releaseId, input, output, biospecimenIdColumn, isPostCGPOnly)
      Variants.run(studyId, releaseId, input, output)
      Consequences.run(studyId, releaseId, input, output)
    }
    else if (runType == "occurrences")
      Occurrences.run(studyId, releaseId, input, output, biospecimenIdColumn, isPostCGPOnly)
    else if (runType == "variants")
      Variants.run(studyId, releaseId, input, output)
    else if (runType == "consequences")
      Consequences.run(studyId, releaseId, input, output)
  }


}

