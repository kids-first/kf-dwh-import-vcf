package org.kidsfirstdrc.dwh.vcf

import bio.ferlab.datalake.spark3.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETL
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.Clinical
import org.kidsfirstdrc.dwh.utils.SparkUtils._

class Occurrences(studyId: String, releaseId: String)(implicit conf: Configuration) extends ETL() {

  val destination = Clinical.occurrences

  override def extract()(implicit spark: SparkSession): Map[String, DataFrame] = {
    val occurrences_family =
      spark.read.parquet(
        s"${Clinical.occurrences_family.rootPath}/occurrences_family/occurrences_family_${studyId.toLowerCase}_${releaseId.toLowerCase}"
      )
    Map(
      Clinical.occurrences_family.id -> occurrences_family
    )
  }

  override def transform(data: Map[String, DataFrame])(implicit spark: SparkSession): DataFrame = {
    data(Clinical.occurrences_family.id)
  }

  override def load(data: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val tableOccurence = tableName(destination.id, studyId, releaseId)
    data
      .repartitionByRange(700, $"has_alt", $"dbgap_consent_code", $"chromosome", $"start")
      .write
      .mode("overwrite")
      .partitionBy("study_id", "has_alt", "dbgap_consent_code", "chromosome")
      .format("parquet")
      .option("path", s"${destination.rootPath}/${destination.id}/$tableOccurence")
      .saveAsTable(tableOccurence)

    data
  }

  override def run()(implicit spark: SparkSession): DataFrame = {
    val outputDf = transform(extract())
    load(outputDf)
  }
}
