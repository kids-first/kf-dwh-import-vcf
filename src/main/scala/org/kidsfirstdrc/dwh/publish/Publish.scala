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
        publishDataservice(releaseId, "variant", "variant_live")
        publishTable(releaseId, "variants")
        publishTable(releaseId, "variants", "variant", s"variant_live")
        publishTable(releaseId, "consequences")
        publishTable(releaseId, "consequences", "variant", s"variant_live")
      case _ =>
        publishOccurrences(studyIds, releaseId, schema)
        publishTable(releaseId, "variants", schema)
        publishTable(releaseId, "consequences", schema)
    }
  }

  private def publishDataservice(releaseId: String, schemaFrom: String = "variant", schemaTo: String = "variant"): Unit = {
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
      publishTable(releaseId, t, schemaFrom, schemaTo)
    }
  }

  def publishTable(releaseId: String, tableName: String, schemaFrom: String = "variant", schemaTo: String = "variant")(implicit spark: SparkSession): Unit = {
    spark.sql(
      s"create or replace view $schemaTo.$tableName as select * from $schemaFrom.${tableName}_${releaseId.toLowerCase()}"
    )
  }

  private def publishOccurrences(studyIds: String, releaseId: String, schemaFrom: String = "variant")(implicit
                                                                                                      spark: SparkSession
  ): Unit = {
    val allStudies = studyIds.split(",")
    allStudies
      .foreach { study =>
        val studyLc = study.toLowerCase
        spark.sql(
          s"create or replace VIEW $schemaFrom.occurrences_family_${studyLc} AS SELECT * FROM $schemaFrom.occurrences_${studyLc}_${releaseId.toLowerCase}"
        )
        spark.sql(
          s"create or replace VIEW $schemaFrom.occurrences_${studyLc} AS SELECT * FROM $schemaFrom.occurrences_${studyLc}_${releaseId.toLowerCase}"
        )
      }
  }
}
