package org.kidsfirstdrc.dwh.vcf

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.kidsfirstdrc.dwh.utils.SparkUtils._
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns._

object Consequences {
  def run(studyId: String, releaseId: String, input: String, output: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    val inputDF = unionCGPFiles(input, studyId, releaseId)
    val consequences = build(studyId, releaseId, inputDF)

    val tableConsequences = tableName("consequences", studyId, releaseId)
    val salt = (rand * 3).cast(IntegerType)  //3 files per chr, tried with 1 file per chr but got an OOM when writing parquet files
    consequences
      .repartition(69, $"chromosome", salt)
      .write.mode(SaveMode.Overwrite)
      .partitionBy("study_id", "release_id", "chromosome")
      .format("parquet")
      .option("path", s"$output/consequences/$tableConsequences")
      .saveAsTable(tableConsequences)

  }


  /**
   * This function is a hack for loading both CGP and postCGP files.
   * These 2 kinds of vcf does not have the same headers, so it results in error when trying to parse these files together
   * That's why we need to address schema differences, and then union these 2 dataframes manually.
   *
   * @param input path where are located the files
   * @param studyId studyId
   * @param releaseId releaseId
   * @param spark session
   * @return a dataframe that unions cgp and postcgp vcf
   */
  private def unionCGPFiles(input: String, studyId: String, releaseId: String)(implicit spark: SparkSession): DataFrame = {
    (postCGPExist(input), cgpExist(input)) match {
      case (false, true) => loadConsequences(cgpFiles(input), studyId, releaseId)
      case (true, false) => loadConsequences(postCGPFiles(input), studyId, releaseId)
      case (true, true) =>
        val postCGP = loadConsequences(postCGPFiles(input), studyId, releaseId)
        val cgp = loadConsequences(cgpFiles(input), studyId, releaseId)
        union(postCGP, cgp)
      case (false, false) => throw new IllegalStateException("No VCF files found!")
    }

  }

  private def loadConsequences(input: String, studyId: String, releaseId: String)(implicit spark: SparkSession): DataFrame = {
    visibleVcf(input, studyId, releaseId)
      .select(
        chromosome,
        start,
        end,
        reference,
        alternate,
        name,
        annotations
      )
  }

  def build(studyId: String, releaseId: String, inputDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val consequencesDF = inputDF
      .groupBy(locus: _*)
      .agg(
        first("annotations") as "annotations",
        first("name") as "name",
        first("end") as "end"
      )
      .withColumn("annotation", explode($"annotations"))
      .drop("annotations")
      .select($"*",
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
        canonical,
        lit(studyId) as "study_id",
        lit(releaseId) as "release_id"
      )
      .drop("annotation")

    consequencesDF
  }
}
