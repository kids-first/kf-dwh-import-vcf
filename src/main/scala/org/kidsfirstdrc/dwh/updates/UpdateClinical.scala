package org.kidsfirstdrc.dwh.updates

import bio.ferlab.datalake.spark3.config.{Configuration, DatasetConf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.{Clinical, Public}
import org.kidsfirstdrc.dwh.jobs.StandardETL
import org.kidsfirstdrc.dwh.join.JoinConsequences._
import org.kidsfirstdrc.dwh.join.JoinWrite.write
import org.kidsfirstdrc.dwh.publish.Publish.publishTable
import org.kidsfirstdrc.dwh.utils.ClinicalUtils._
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns.locusColumNames

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.util.Try

class UpdateClinical(source: DatasetConf, destination: DatasetConf, schema: String)(implicit conf: Configuration)
    extends StandardETL(destination)(conf) {

  override def extract()(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      destination.id -> spark.table(s"$schema.${destination.id}"),
      //TODO remove .dropDuplicates(locusColumNames) when issue#2893 is fixed
      source.id -> source.read.dropDuplicates(locusColumNames)
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

  private def updateGnomad311(data: Map[String, DataFrame])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val variant = data(destination.id)
    val gnomad311 = data(Public.gnomad_genomes_3_1_1.id)
      .selectLocus($"ac", $"an", $"af", $"nhomalt" as "hom")

    variant
      .drop("gnomad_genomes_3_1_1")
      .joinAndMerge(gnomad311, "gnomad_genomes_3_1_1")
  }

  private def updateDbnsfp(data: Map[String, DataFrame])(implicit spark: SparkSession): DataFrame = {

    val consequences = data(destination.id)
    val dbnsfp = data(Public.dbnsfp_original.id)
      .drop(
        "aaref",
        "symbol",
        "ensembl_gene_id",
        "ensembl_protein_id",
        "VEP_canonical",
        "cds_strand")

    val columnsToKeep = Seq("chromosome", "start", "reference", "alternate", "ensembl_transcript_id")
    val columnsToDrop = dbnsfp.columns.filterNot(columnsToKeep.contains)

    consequences
      .drop(columnsToDrop:_*)
      .joinWithDbnsfp(dbnsfp)
  }

  override def transform(data: Map[String, DataFrame])(implicit spark: SparkSession): DataFrame = {
    (source, destination) match {
      case (Public.clinvar             , Clinical.variants)     => updateClinvar(data)
      case (Public.topmed_bravo        , Clinical.variants)     => updateTopmed(data)
      case (Public.gnomad_genomes_3_1_1, Clinical.variants)     => updateGnomad311(data)
      case (Public.dbnsfp_original     , Clinical.consequences) => updateDbnsfp(data)
      case _                           => throw new IllegalArgumentException(s"Nothing to do for ${source.id} -> ${destination.id}")
    }
  }

  override def load(data: DataFrame)(implicit spark: SparkSession): DataFrame = {

    val localTimeNow       = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))
    val releaseId_datetime = s"${lastReleaseId}_$localTimeNow"

    write(releaseId_datetime, destination.rootPath, destination.id, data, Some(60), schema)
    publishTable(releaseId_datetime, destination.id, schema)
    data
  }

  override def run()(implicit spark: SparkSession): DataFrame = {
    Try(extract())
      .map {inputs =>
        val output = transform(inputs)
        val finalDf = load(output)
        publish()
        finalDf
      }.fold(_ => spark.emptyDataFrame, identity)
  }
}
