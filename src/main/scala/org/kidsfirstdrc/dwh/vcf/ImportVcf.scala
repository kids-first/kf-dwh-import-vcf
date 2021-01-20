package org.kidsfirstdrc.dwh.vcf

import org.apache.spark.sql.SparkSession

import scala.util.Try

object ImportVcf extends App {

  println(s"Job arguments: ${args.mkString("[", ", ", "]")}")

  val Array(studyId, releaseId, input, output, runType, biospecimenIdColumn, isPatternOverride) = args

  implicit val spark: SparkSession = SparkSession.builder
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .appName(s"Import $runType for $studyId - $releaseId").getOrCreate()

  val isPatternOverriden: Boolean = Try(isPatternOverride.toBoolean).getOrElse(false)

  run(studyId, releaseId, input, output, runType, biospecimenIdColumn, isPatternOverriden)

  def run(studyId: String, releaseId: String, input: String, output: String,
          runType: String = "all", biospecimenIdColumn: String = "biospecimen_id", isPatternOverriden: Boolean = false)
         (implicit spark: SparkSession): Unit = {
    spark.sql("use variant")

    runType match {
      case "occurrences" => Occurrences.run(studyId, releaseId, input, output, biospecimenIdColumn, isPatternOverriden)
      case "variants" => Variants.run(studyId, releaseId, input, output)
      case "consequences" => Consequences.run(studyId, releaseId, input, output)
      case "all" =>
        Occurrences.run(studyId, releaseId, input, output, biospecimenIdColumn, isPatternOverriden)
        Variants.run(studyId, releaseId, input, output)
        Consequences.run(studyId, releaseId, input, output)

    }
  }

}

