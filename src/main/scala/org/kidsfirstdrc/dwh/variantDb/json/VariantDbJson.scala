package org.kidsfirstdrc.dwh.variantDb.json

import org.apache.spark.sql.functions.{explode, _}
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.kidsfirstdrc.dwh.external.omim.ImportOmimGeneSet
import org.kidsfirstdrc.dwh.join.JoinConsequences
import org.kidsfirstdrc.dwh.utils.EtlJob
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns.locus
import org.kidsfirstdrc.dwh.vcf.Variants

object VariantDbJson extends EtlJob {

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
      .select("chromosome", "start", "reference", "alternate", "symbol", "ensembl_gene_id", "consequences",
        "name", "impact", "symbol", "strand", "biotype", "variant_class", "exon", "intron", "hgvsc", "hgvsp", "hgvsg",
        "cds_position", "cdna_position", "protein_position", "amino_acids", "codons", "canonical", "aa_change", "coding_dna_change",
        "ensembl_transcript_id", "ensembl_regulatory_id", "feature_type")

    val omim = data(ImportOmimGeneSet.TABLE_NAME)
      .select("ensembl_gene_id", "entrez_gene_id", "omim_gene_id")

    val consequenceColumns: Set[String] = consequences.columns.toSet ++ omim.columns.toSet -- Set("chromosome", "start", "reference", "alternate")

    val consequencesWithOmim = consequences
      .join(omim, Seq("ensembl_gene_id"), "left")
      .withColumn("consequence", struct(consequenceColumns.toSeq.map(c => col(c)):_*))
      .groupBy(locus:_*)
      .agg(collect_set(col("consequence")).as("consequences"))

    variants
      .withStudies
      .withFrequencies
      .withClinVar
      .select("chromosome", "start", "end", "reference", "alternate", "studies", "frequencies", "clinvar", "dbsnp_id")
      .joinByLocus(consequencesWithOmim)

    //todo transform roughly to this:

    /*
    {
    "chromosome":"1",
    "start": 1000,
    "end": 1010,
    "reference":"A",
    "alternate":"C",
    "studies" : [
        "study_id": "SD_ABC",
        "consent_codes": ["phs001.c1", ...],
        "frequencies" : {
            "hmb" : {...},
            "gru" : {...},
        }
    ],
    "frequencies" : {
            "gnomad_3_0": {
                "ac": 1,
                "an": 2,
                "af": 0.5,
                "hom": 12,
                "het": 10,
                "participants": 1
            },
            "gnomad_2_1": { ... },
            "gnomad_2_1_exome":  {... },
            "topmed": { ... },
            "1000_genomes": { ... },
            "internal" : {
                "hmb" : {
                    "ac":1,
                    "af":0.23,
                    ...
                },
                "gru": {
                 .....
                }
            }
    },
    "clinvar" : {
        "name" : "...",
        "clin_sig": ["...."]
    },
    "dbsnp_id": "...",
    //Ci dessous jkoin avec la table consequence
    "consequences": [
        {
           "symbol": "BRAF1",
           "ensembl_gene_id": "ENS000001",
           "entrez_gene_id": 12345",
           "omim_gene_id": "23234234",
           "consequences": ["upstream_gene", "exon"],
           "name" : "...."
           "impact" : "...."
           "symbol" : "...."
           "ensembl_gene_id" : "...."
           "strand" : "...."
           "biotype" : "...."
           "variant_class" : "...."
           "exon" : "...."
           "intron" : "...."
           "hgvsc" : "...."
           "hgvsp" : "...."
           "hgvsg" : "...."
           "cds_position" : "...."
           "cdna_position" : "...."
           "protein_position" : "...."
           "amino_acids" : "...."
           "codons" : "...."
           "canonical" : true,
           "aa_change" : "...",
           "coding_dna_change": "...",
           "ensembl_transcript_id": " ...",
           "ensembl_regulatory_id": "...",
           "feature_type": " ...",
           "scores": {
                "CADD" ... voire clin
           }
        }
    ]
     */
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
