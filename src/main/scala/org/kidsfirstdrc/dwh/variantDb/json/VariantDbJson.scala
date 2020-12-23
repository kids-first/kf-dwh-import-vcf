package org.kidsfirstdrc.dwh.variantDb.json

import org.apache.spark.sql.functions.{explode, _}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.kidsfirstdrc.dwh.external.omim.ImportOmimGeneSet
import org.kidsfirstdrc.dwh.join.JoinConsequences
import org.kidsfirstdrc.dwh.utils.MultiSourceEtlJob
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns.locus
import org.kidsfirstdrc.dwh.vcf.Variants

object VariantDbJson extends MultiSourceEtlJob {

  private def frequenciesFor(prefix: String): Column = {
    struct(
      struct(
        col(s"${prefix}_ac") as "ac",
        col(s"${prefix}_an") as "an",
        col(s"${prefix}_af") as "af",
        col(s"${prefix}_homozygotes") as "homozygotes",
        col(s"${prefix}_heterozygotes") as "heterozygotes"
      ).as(prefix)
    )
  }

  private def frequenciesByStudiesFor(prefix: String): Column = {
    struct(
      struct(
        col(s"${prefix}_ac_by_study")(col("study_id")) as "ac",
        col(s"${prefix}_an_by_study")(col("study_id")) as "an",
        col(s"${prefix}_af_by_study")(col("study_id")) as "af",
        col(s"${prefix}_homozygotes_by_study")(col("study_id")) as "homozygotes",
        col(s"${prefix}_heterozygotes_by_study")(col("study_id")) as "heterozygotes"
      ).as(prefix)
    )
  }

  implicit class DataFrameOperations(df: DataFrame) {

    def joinByLocus(df2: DataFrame): DataFrame = {
      df.join(df2, locus.map(_.toString), "left")
    }

    def withStudies: DataFrame = {
      val inputColumns: Seq[Column] = df.columns.map(col)
      df
        .select(inputColumns :+ explode(col("studies")).as("study_id"):_*)
        .withColumn("consent_codes", col("consent_codes_by_study")(col("study_id")))
        .withColumn("studies", struct(
          col("study_id"),
          col("consent_codes"),
          frequenciesByStudiesFor("hmb"),
          frequenciesByStudiesFor("gru")))
        .groupBy(locus:_*)
        .agg(collect_set("studies"), (inputColumns.toSet -- locus.toSet).map(c => first(c).as(c.toString)).toList:_*)
    }

    def withFrequencies: DataFrame = {
      df
        .withColumn("frequencies", struct(
          col("one_k_genomes").as("1k_genomes"),
          col("topmed"),
          col("gnomad_genomes_2_1"),
          col("gnomad_exomes_2_1"),
          col("gnomad_genomes_3_0"),
          frequenciesFor("hmb"),
          frequenciesFor("gru")
        ))
    }

    def withClinVar: DataFrame = {
      df
        .withColumn("clinvar", struct(
          col("clinvar_id").as("name"),
          col("clin_sig").as("clin_sig")
        ))
    }
  }

  val tableName = "variants"

  override def extract(input: String)(implicit spark: SparkSession): Map[String, DataFrame] = {
    //TODO figure out how to make the job idempontent
    Map(
      Variants.TABLE_NAME -> spark.table(Variants.TABLE_NAME),
      JoinConsequences.TABLE_NAME -> spark.table(JoinConsequences.TABLE_NAME),
      ImportOmimGeneSet.TABLE_NAME -> spark.table(ImportOmimGeneSet.TABLE_NAME)
    )
  }

  override def transform(data: Map[String, DataFrame])(implicit spark: SparkSession): DataFrame = {
    val variants = data(Variants.TABLE_NAME)

    val consequences = data(JoinConsequences.TABLE_NAME)

    val consequencesColumns = Set("chromosome", "start", "reference", "alternate", "symbol", "ensembl_gene_id", "consequences",
        "name", "impact", "symbol", "strand", "biotype", "variant_class", "exon", "intron", "hgvsc", "hgvsp", "hgvsg",
        "cds_position", "cdna_position", "protein_position", "amino_acids", "codons", "canonical", "aa_change", "coding_dna_change",
        "ensembl_transcript_id", "ensembl_regulatory_id", "feature_type", "scores")

    val scoresColumns = consequences.schema.fields.filter(_.dataType.equals(DoubleType)).map(_.name).toSet

    val omim = data(ImportOmimGeneSet.TABLE_NAME)
      .select("ensembl_gene_id", "entrez_gene_id", "omim_gene_id")

    val consequenceColumnsWithOmim: Set[String] = consequencesColumns ++ omim.columns.toSet -- Set("chromosome", "start", "reference", "alternate")

    val consequencesWithOmim = consequences
      .withColumn("scores", struct(scoresColumns.toSeq.map(col):_*))
      .join(omim, Seq("ensembl_gene_id"), "left")
      .withColumn("consequence", struct(consequenceColumnsWithOmim.toSeq.map(col):_*))
      .groupBy(locus:_*)
      .agg(collect_set(col("consequence")).as("consequences"))

    variants
      .withStudies
      .withFrequencies
      .withClinVar
      .select("chromosome", "start", "end", "reference", "alternate", "studies", "frequencies", "clinvar", "dbsnp_id")
      .joinByLocus(consequencesWithOmim)
  }

  override def load(data: DataFrame, output: String)(implicit spark: SparkSession): Unit = {
    data
      .write.mode(SaveMode.Overwrite)
      .partitionBy("release_id")
      .format("json")
      .option("path", s"$output/variantdb/$tableName")
      .saveAsTable(tableName)
  }

}
