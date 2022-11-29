package org.kidsfirstdrc.dwh.updates

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf, RunStep}
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns.locusColumNames
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.{Clinical, Public}
import org.kidsfirstdrc.dwh.jobs.StandardETL
import org.kidsfirstdrc.dwh.join.JoinConsequences._
import org.kidsfirstdrc.dwh.join.JoinWrite.write
import org.kidsfirstdrc.dwh.publish.Publish.publishTable
import org.kidsfirstdrc.dwh.utils.ClinicalUtils._

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.util.Try

class UpdateClinical(source: DatasetConf, destination: DatasetConf, schema: String)(implicit conf: Configuration)
  extends StandardETL(destination)(conf) {

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      destination.id -> spark.table(s"$schema.${destination.id}"),
      //TODO remove .dropDuplicates(locusColumNames) when issue#2893 is fixed
      source.id -> source.read.dropDuplicates(locusColumNames)
    )
  }

  private def updateClinvar(data: Map[String, DataFrame])(implicit spark: SparkSession): DataFrame = {
    val variant = data(destination.id).drop("clinvar_id", "clin_sig")
    val clinvar = data(Public.clinvar.id)
    variant
      .joinByLocus(clinvar, "left")
      .select(variant("*"), clinvar("name") as "clinvar_id", clinvar("clin_sig") as "clin_sig")
  }

  private def genericUpdate(data: Map[String, DataFrame],
                            inputColumns: Seq[Column],
                            outputColumn: String)(implicit spark: SparkSession): DataFrame = {
    val sourceDf = data(source.id)
      .selectLocus(inputColumns:_*)
    //.selectLocus($"ac", $"an", $"af")

    data(destination.id)
      .drop(outputColumn)
      .joinAndMerge(sourceDf, outputColumn, "left")
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

  override def transform(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    (source, destination) match {
      case (Public.clinvar             , Clinical.variants)     => updateClinvar(data)
      case (Public.topmed_bravo        , Clinical.variants)     => genericUpdate(data, Seq($"ac", $"an", $"af", $"homozygotes", $"heterozygotes"), "topmed")
      case (Public.gnomad_exomes_2_1   , Clinical.variants)     => genericUpdate(data, Seq($"ac", $"an", $"af", $"hom"), "gnomad_exomes_2_1")
      case (Public.gnomad_genomes_2_1  , Clinical.variants)     => genericUpdate(data, Seq($"ac", $"an", $"af", $"hom"), "gnomad_genomes_2_1")
      case (Public.gnomad_genomes_3_0  , Clinical.variants)     => genericUpdate(data, Seq($"ac", $"an", $"af", $"hom"), "gnomad_genomes_3_0")
      case (Public.gnomad_genomes_3_1_1, Clinical.variants)     => genericUpdate(data, Seq($"ac", $"an", $"af", $"nhomalt" as "hom"), "gnomad_genomes_3_1_1")
      case (Public.`1000_genomes`      , Clinical.variants)     => genericUpdate(data, Seq($"ac", $"an", $"af"), "1k_genomes")
      case (Public.dbnsfp_original     , Clinical.consequences) => updateDbnsfp(data)
      case _                           => throw new IllegalArgumentException(s"Nothing to do for ${source.id} -> ${destination.id}")
    }
  }

  override def load(data: DataFrame,
                    lastRunDateTime: LocalDateTime = minDateTime,
                    currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {

    val localTimeNow       = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))
    val releaseId_datetime = s"${lastReleaseId}_$localTimeNow"

    write(releaseId_datetime, destination.rootPath, destination.id, data, Some(60), schema)
    publishTable(releaseId_datetime, destination.id, schema)
    data
  }

  override def run(runSteps: Seq[RunStep] = RunStep.default_load,
                   lastRunDateTime: Option[LocalDateTime] = None,
                   currentRunDateTime: Option[LocalDateTime] = None)(implicit spark: SparkSession): DataFrame = {
    Try(extract())
      .map {inputs =>
        val output = transform(inputs)
        val finalDf = load(output)
        publish()
        finalDf
      }.fold(_ => spark.emptyDataFrame, identity)
  }
}
