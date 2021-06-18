package org.kidsfirstdrc.dwh.covirt

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.kidsfirstdrc.dwh.join.JoinConsequences
import org.kidsfirstdrc.dwh.utils.SparkUtils._
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns._
import JoinConsequences._

object Consequences {
  def run(releaseId: String, input: String, output: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    val inputDF                    = vcf(input, None)
    val consequences: DataFrame    = build(inputDF)
    val dbnsfp_original: DataFrame = spark.table("variant.dbnsfp_original")

    val joinConsequences  = consequences.joinWithDbnsfp(dbnsfp_original)
    val tableConsequences = s"consequences_${releaseId.toLowerCase}"
    joinConsequences
      .repartition($"chromosome")
      .sortWithinPartitions("start")
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("chromosome")
      .format("parquet")
      .option("path", s"$output/consequences/covirt/$tableConsequences")
      .saveAsTable(tableConsequences)

  }

  def build(inputDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val consequencesDF = inputDF
      .select(
        chromosome,
        start,
        end,
        reference,
        alternate,
        name,
        annotations
      )
      .groupBy(locus: _*)
      .agg(
        first("annotations") as "annotations",
        first("name") as "name"
      )
      .withColumn("annotation", explode($"annotations"))
      .drop("annotations")
      .select(
        $"*",
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
        codons
      )
      .drop("annotation")
      .withColumn("consequence", explode($"consequences"))
      .drop("consequences")

    consequencesDF
  }
}
