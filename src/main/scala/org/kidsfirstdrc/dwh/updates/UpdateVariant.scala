package org.kidsfirstdrc.dwh.updates

import bio.ferlab.datalake.spark3.config.Configuration
import bio.ferlab.datalake.spark3.config.DatasetConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.{Clinical, Public}
import org.kidsfirstdrc.dwh.jobs.StandardETL
import org.kidsfirstdrc.dwh.join.JoinWrite.write
import org.kidsfirstdrc.dwh.publish.Publish.publishTable
import org.kidsfirstdrc.dwh.utils.ClinicalUtils._
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns.locusColumNames

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class UpdateVariant(source: DatasetConf, schema: String)(implicit conf: Configuration)
    extends StandardETL(Clinical.variants)(conf) {

  override def extract()(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      destination.id -> spark.table(s"$schema.${destination.id}"),
      //TODO remove .dropDuplicates(locusColumNames) when issue#2893 is fixed
      source.id -> spark.table(s"variant.${source.id}").dropDuplicates(locusColumNames)
    )
  }

  private def updateClinvar(
      data: Map[String, DataFrame]
  )(implicit spark: SparkSession): DataFrame = {
    val variant = data(destination.id).drop("clinvar_id", "clin_sig")
    val clinvar = data(Public.clinvar.id)
    variant
      .joinByLocus(clinvar, "left")
      .select(variant("*"), clinvar("name") as "clinvar_id", clinvar("clin_sig") as "clin_sig")
  }

  private def updateTopmed(
      data: Map[String, DataFrame]
  )(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val variant = data(destination.id)
    val topmed = data(Public.topmed_bravo.id)
      .selectLocus($"ac", $"an", $"af", $"homozygotes", $"heterozygotes")
    variant
      .drop("topmed")
      .joinAndMerge(topmed, "topmed")
  }

  private def updateGnomad311(
      data: Map[String, DataFrame]
  )(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val variant = data(destination.id)
    val gnomad311 = data(Public.gnomad_genomes_3_1_1.id)
      .selectLocus($"ac", $"an", $"af", $"nhomalt" as "hom")
    variant
      .withColumnRenamed("gnomad_genomes_3_0", "gnomad_genomes_3_1_1")
      .drop("gnomad_genomes_3_1_1")
      .joinAndMerge(gnomad311, "gnomad_genomes_3_1_1")
  }

  override def transform(data: Map[String, DataFrame])(implicit spark: SparkSession): DataFrame = {
    source match {
      case Public.clinvar              => updateClinvar(data)
      case Public.topmed_bravo         => updateTopmed(data)
      case Public.gnomad_genomes_3_1_1 => updateGnomad311(data)
      case _                           => throw new IllegalArgumentException(s"Nothing to do for $source")
    }
  }

  override def load(data: DataFrame)(implicit spark: SparkSession): DataFrame = {

    val localTimeNow       = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))
    val releaseId_datetime = s"${lastReleaseId}_$localTimeNow"

    write(releaseId_datetime, destination.rootPath, destination.id, data, Some(60), schema)
    publishTable(releaseId_datetime, destination.id)
    data
  }
}
