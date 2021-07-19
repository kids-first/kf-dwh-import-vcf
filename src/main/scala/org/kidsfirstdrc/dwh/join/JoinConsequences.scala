package org.kidsfirstdrc.dwh.join

import bio.ferlab.datalake.spark3.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETL
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.{Clinical, Public}
import org.kidsfirstdrc.dwh.join.JoinConsequences._
import bio.ferlab.datalake.spark3.implicits.SparkUtils
import bio.ferlab.datalake.spark3.implicits.SparkUtils.firstAs

class JoinConsequences(
    studyIds: Seq[String],
    releaseId: String,
    mergeWithExisting: Boolean,
    database: String
)(implicit conf: Configuration)
    extends ETL() {

  override def extract()(implicit spark: SparkSession): Map[String, DataFrame] = {
    val consequences: DataFrame = studyIds.foldLeft(spark.emptyDataFrame) { (currentDF, studyId) =>
      val nextDf =
        spark.table(SparkUtils.tableName(Clinical.consequences.id, studyId, releaseId, database))
      if (currentDF.isEmpty)
        nextDf
      else {
        currentDF
          .union(nextDf)
      }
    }

    Map(
      Clinical.consequences.id  -> consequences,
      Public.dbnsfp_original.id -> spark.table("variant.dbnsfp_original")
    )
  }

  override def transform(data: Map[String, DataFrame])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val consequences    = data(Clinical.consequences.id)
    val dbnsfp_original = data(Public.dbnsfp_original.id)
      .drop(
        "aaref",
        "symbol",
        "ensembl_gene_id",
        "ensembl_protein_id",
        "VEP_canonical",
        "cds_strand")

    val commonColumns = Seq(
      $"chromosome",
      $"start",
      $"end",
      $"reference",
      $"alternate",
      $"consequences",
      $"ensembl_transcript_id",
      $"ensembl_regulatory_id",
      $"feature_type",
      $"name",
      $"impact",
      $"symbol",
      $"ensembl_gene_id",
      $"strand",
      $"biotype",
      $"variant_class",
      $"exon",
      $"intron",
      $"hgvsc",
      $"hgvsp",
      $"hgvsg",
      $"cds_position",
      $"cdna_position",
      $"protein_position",
      $"amino_acids",
      $"codons",
      $"original_canonical",
      $"canonical",
      $"mane_plus",
      $"mane_select",
      $"refseq_mrna_id",
      $"refseq_protein_id"
    )

    val allColumns = commonColumns :+ col("study_id")

    val merged =
      if (mergeWithExisting && spark.catalog.tableExists(s"${Clinical.consequences.id}")) {
        val existingConsequences = spark.table(s"${Clinical.consequences.id}")

        val existingColumns = commonColumns :+ $"study_ids"
        mergeConsequences(
          releaseId,
          existingConsequences
            .select(existingColumns: _*)
            .withColumn("study_id", explode($"study_ids"))
            .drop("study_ids")
            .where(not($"study_id".isin(studyIds: _*)))
            .union(consequences.select(allColumns: _*))
        )
      } else {
        mergeConsequences(releaseId, consequences.select(allColumns: _*))
      }

    merged.joinWithDbnsfp(dbnsfp_original)

  }

  override def load(data: DataFrame)(implicit spark: SparkSession): DataFrame = {
    JoinWrite.write(
      releaseId,
      Clinical.consequences.rootPath,
      Clinical.consequences.id,
      data,
      Some(30),
      database
    )
  }

  private def mergeConsequences(releaseId: String, consequences: DataFrame)(implicit
      spark: SparkSession
  ): DataFrame = {

    import spark.implicits._

    consequences
      .groupBy(
        $"chromosome",
        $"start",
        $"end",
        $"reference",
        $"alternate",
        $"ensembl_transcript_id",
        $"ensembl_regulatory_id",
        $"feature_type"
      )
      .agg(
        firstAs("consequences"),
        firstAs("name"),
        firstAs("impact"),
        firstAs("symbol"),
        firstAs("ensembl_gene_id"),
        firstAs("strand"),
        firstAs("biotype"),
        firstAs("variant_class"),
        firstAs("exon"),
        firstAs("intron"),
        firstAs("hgvsc"),
        firstAs("hgvsp"),
        firstAs("hgvsg"),
        firstAs("cds_position"),
        firstAs("cdna_position"),
        firstAs("protein_position"),
        firstAs("amino_acids"),
        firstAs("codons"),
        firstAs("original_canonical"),
        firstAs("canonical"),
        firstAs("mane_plus"),
        firstAs("mane_select"),
        firstAs("refseq_mrna_id"),
        firstAs("refseq_protein_id"),
        collect_set($"study_id") as "study_ids"
      )
      .withColumn(
        "aa_change",
        when(
          $"amino_acids".isNotNull,
          concat($"amino_acids.reference", $"protein_position", $"amino_acids.variant")
        ).otherwise(lit(null))
      )
      .withColumn(
        "coding_dna_change",
        when(
          $"cds_position".isNotNull,
          concat($"cds_position", $"reference", lit(">"), $"alternate")
        ).otherwise(lit(null))
      )
      .withColumn("release_id", lit(releaseId))

  }

  override val destination: DatasetConf = Clinical.consequences
}

object JoinConsequences {
  implicit class DataFrameOperations(df: DataFrame) {
    def joinWithDbnsfp(dbnsfp_original: DataFrame): DataFrame = {
      df
        .join(dbnsfp_original, Seq("chromosome", "start", "reference", "alternate", "ensembl_transcript_id"), "left")
    }
  }
}
