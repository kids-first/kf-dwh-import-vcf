package org.kidsfirstdrc.dwh.vcf

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf, RunType}
import bio.ferlab.datalake.spark3.etl.ETL
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.vcf
import bio.ferlab.datalake.spark3.implicits.SparkUtils._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.{Clinical, HarmonizedData, Public}
import org.kidsfirstdrc.dwh.utils.ClinicalUtils.getVisibleFiles

import java.time.LocalDateTime

class Consequences(studyId: String,
                   releaseId: String,
                   cgpPattern: String,
                   postCgpPattern: String,
                   referenceGenomePath: Option[String] = None)(implicit conf: Configuration)
  extends ETL() {

  val destination: DatasetConf = Clinical.consequences

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    val inputDF = vcf(
      (getVisibleFiles(studyId, releaseId, cgpPattern) ++
        getVisibleFiles(studyId, releaseId, postCgpPattern)).distinct,
      referenceGenomePath
    )
      .withColumn("file_name", filename)
      .select(chromosome, start, end, reference, alternate, name, annotations, is_normalized)

    Map(
      HarmonizedData.family_variants_vcf.id -> inputDF
    )
  }

  override def transform(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    val consequencesDf = data(HarmonizedData.family_variants_vcf.id)
      .groupBy(locus: _*)
      .agg(
        first("annotations") as "annotations",
        first("name") as "name",
        first("end") as "end",
        first("is_normalized") as "is_normalized"
      )
      .withColumn("annotation", explode(col("annotations")))
      .drop("annotations")
      .select(
        col("*"),
        consequences,
        impact,
        symbol,
        ensembl_gene_id,
        ensembl_transcript_id,
        ensembl_regulatory_id,
        feature_type,
        strand,
        biotype,
        variant_class,
        exon,
        intron,
        hgvsc,
        hgvsp,
        hgvsg,
        cds_position,
        cdna_position,
        protein_position,
        amino_acids,
        codons,
        original_canonical,
        lit(studyId) as "study_id",
        lit(releaseId) as "release_id"
      )
      .drop("annotation")
    consequencesDf
  }

  override def load(data: DataFrame,
                    lastRunDateTime: LocalDateTime = minDateTime,
                    currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    val tableConsequences = tableName(destination.id, studyId, releaseId)
    val salt = (rand * 3).cast(
      IntegerType
    ) //3 files per chr, tried with 1 file per chr but got an OOM when writing parquet files
    data
      .repartition(70, col("chromosome"), salt)
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("study_id", "release_id", "chromosome")
      .format("parquet")
      .option("path", s"${destination.rootPath}/${destination.id}/$tableConsequences")
      .saveAsTable(tableConsequences)
    data
  }

  override def run(runType: RunType)(implicit spark: SparkSession): DataFrame = {
    val input = extract()

    val consequences = transform(input)

    load(consequences)
  }
}
