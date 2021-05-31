package org.kidsfirstdrc.dwh.publish

import org.apache.spark.sql.SparkSession

object Publish extends App {

  val Array(studyIds, releaseId) = args

  implicit val spark: SparkSession = SparkSession.builder
    .config(
      "hive.metastore.client.factory.class",
      "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
    )
    .enableHiveSupport()
    .appName(s"Publishing for $studyIds - $releaseId")
    .getOrCreate()

  run(studyIds, releaseId)

  def run(studyIds: String, releaseId: String)(implicit spark: SparkSession): Unit = {
    publishOccurrences(studyIds, releaseId)
    publishDataservice(releaseId)
    publishTable(releaseId, "variants")
    publishTable(releaseId, "consequences")
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

  def publishTable(releaseId: String, tableName: String)(implicit spark: SparkSession): Unit = {

    spark.sql(
      s"create or replace view variant_live.$tableName as select * from variant.${tableName}_${releaseId.toLowerCase()}"
    )
    spark.sql(
      s"create or replace view variant.$tableName as select * from variant.${tableName}_${releaseId.toLowerCase()}"
    )

  }

  private def publishOccurrences(studyIds: String, releaseId: String)(implicit
      spark: SparkSession
  ): Unit = {
    val allStudies = studyIds.split(",")
    allStudies
      .foreach { study =>
        val studyLc = study.toLowerCase
        spark.sql(
          s"create or replace VIEW variant.occurrences_family_${studyLc} AS SELECT * FROM variant.occurrences_${studyLc}_${releaseId.toLowerCase}"
        )
        spark.sql(
          s"create or replace VIEW variant.occurrences_${studyLc} AS SELECT * FROM variant.occurrences_${studyLc}_${releaseId.toLowerCase}"
        )
      }
  }
}
