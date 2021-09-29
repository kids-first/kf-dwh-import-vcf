package org.kidsfirstdrc.dwh.publish

import org.apache.spark.sql.SparkSession

object Publish extends App {

  val Array(studyIds, releaseId, schema) = args

  implicit val spark: SparkSession = SparkSession.builder
    .config(
      "hive.metastore.client.factory.class",
      "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
    )
    .enableHiveSupport()
    .appName(s"Publishing for $studyIds - $releaseId")
    .getOrCreate()

  run(studyIds, releaseId, schema)

  def run(studyIds: String, releaseId: String, schema: String)(implicit spark: SparkSession): Unit = {
    schema match {
      case "variant" =>
        publishOccurrences(studyIds, releaseId)
        publishDataservice(releaseId)
        publishTable(releaseId, "variants", schema)
        publishTable(releaseId, "variants", s"${schema}_live")
        publishTable(releaseId, "consequences", schema)
        publishTable(releaseId, "consequences", s"${schema}_live")
      case _ =>
        publishOccurrences(studyIds, releaseId, schema)
        publishTable(releaseId, "variants", schema)
        publishTable(releaseId, "consequences", schema)
    }
  }

  private def publishDataservice(releaseId: String): Unit = {
    val all = Set(
      "participants",
      "biospecimens",
      "biospecimens_diagnoses",
      "outcomes",
      "investigators",
      "genomic_files",
      "family_relationships",
      "families",
      "diagnoses",
      "sequencing_experiments",
      "studies",
      "outcomes",
      "study_files",
      "phenotypes"
    )
    all.foreach { t =>
      publishTable(releaseId, t)
    }
  }

  def publishTable(releaseId: String, tableName: String, schema: String = "variant")(implicit spark: SparkSession): Unit = {
    spark.sql(
      s"create or replace view $schema.$tableName as select * from $schema.${tableName}_${releaseId.toLowerCase()}"
    )
  }

  private def publishOccurrences(studyIds: String, releaseId: String, schema: String = "variant")(implicit
      spark: SparkSession
  ): Unit = {
    val allStudies = studyIds.split(",")
    allStudies
      .foreach { study =>
        val studyLc = study.toLowerCase
        spark.sql(
          s"create or replace VIEW $schema.occurrences_family_${studyLc} AS SELECT * FROM $schema.occurrences_${studyLc}_${releaseId.toLowerCase}"
        )
        spark.sql(
          s"create or replace VIEW $schema.occurrences_${studyLc} AS SELECT * FROM $schema.occurrences_${studyLc}_${releaseId.toLowerCase}"
        )
      }
  }
}
