package org.kidsfirstdrc.dwh.external.ensembl

import bio.ferlab.datalake.core.config.Configuration
import bio.ferlab.datalake.core.etl.DataSource
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.Public
import org.kidsfirstdrc.dwh.conf.Catalog.Raw._
import org.kidsfirstdrc.dwh.jobs.StandardETL

class ImportEnsemblMapping()(implicit conf: Configuration)
  extends StandardETL(Public.ensembl_mapping)(conf) {

  override def extract()(implicit spark: SparkSession): Map[DataSource, DataFrame] = {
    val tsvWithHeaders = Map("header" -> "true", "sep" -> "\t")
    val tsvWithoutHeaders = Map("header" -> "false", "sep" -> "\t")
    Map(
      ensembl_canonical -> spark.read.options(tsvWithoutHeaders).csv(ensembl_canonical.location),
      ensembl_entrez    -> spark.read.options(tsvWithHeaders).csv(ensembl_entrez.location),
      ensembl_refseq    -> spark.read.options(tsvWithHeaders).csv(ensembl_refseq.location),
      ensembl_uniprot   -> spark.read.options(tsvWithHeaders).csv(ensembl_uniprot.location),
      ensembl_ena       -> spark.read.options(tsvWithHeaders).csv(ensembl_ena.location)
    )
  }

  override def transform(data: Map[DataSource, DataFrame])(implicit spark: SparkSession): DataFrame = {
    val canonical = data(ensembl_canonical)
      .withColumn("ensembl_gene_id", regexp_extract(col("_c0"), "(ENSG[0-9]+)", 0))
      .withColumn("ensembl_transcript_id", regexp_extract(col("_c1"), "(ENST[0-9]+)", 0))
      .withColumnRenamed("_c2", "tag")

    val refseq = data(ensembl_refseq).renameIds.renameExternalReference("refseq")
    val entrez = data(ensembl_entrez).renameIds.renameExternalReference("entrez")
    val uniprot = data(ensembl_uniprot).renameIds.renameExternalReference("uniprot")
    val ena = data(ensembl_ena).renameIds.withColumnRenamed("taxid", "tax_id")



    val joinedDf = canonical
      .join(refseq, Seq("ensembl_gene_id", "ensembl_transcript_id"), "left")
      .join(entrez, Seq("ensembl_gene_id", "ensembl_transcript_id"), "left")
      .join(uniprot, Seq("ensembl_gene_id", "ensembl_transcript_id"), "left")
      .join(ena, Seq("ensembl_gene_id", "ensembl_transcript_id"), "left")

    joinedDf
      .groupBy("ensembl_gene_id", "ensembl_transcript_id")
      .agg(
        collect_list(col("tag")).as("tags"),
        externalIDs(List("refseq", "entrez", "uniprot")) :+
          first("species").as("species") :+
          first("tax_id").as("tax_id") :+
          collect_set("primary_accession").as("primary_accessions") :+
          collect_set("secondary_accession").as("secondary_accessions"):_*
      )
      .withColumn("is_canonical", when(array_contains(col("tags"), "Ensembl Canonical"), lit(true)).otherwise(lit(false)))
      .withColumn("is_mane_select", when(array_contains(col("tags"), "MANE Select v0.93"), lit(true)).otherwise(lit(false)))
      .withColumn("is_mane_plus", when(array_contains(col("tags"), "MANE Plus Clinical v0.93"), lit(true)).otherwise(lit(false)))
      .withColumn("genome_build", lit("GRCh38"))
      .withColumn("ensembl_release_id", lit(104))
  }

  private val externalIDs: List[String] => List[Column] =
    _.map(externalDb => collect_set(struct(
      col(s"${externalDb}_id") as "id",
      col(s"${externalDb}_database") as "database"
    )) as externalDb)

  implicit class DataFrameOps(df: DataFrame) {

    def renameIds: DataFrame = {
      df.withColumnRenamed("gene_stable_id", "ensembl_gene_id")
        .withColumnRenamed("transcript_stable_id", "ensembl_transcript_id")
        .withColumnRenamed("protein_stable_id", "ensembl_protein_id")
    }

    def renameExternalReference(prefix: String): DataFrame = {
      df.withColumnRenamed("xref", s"${prefix}_id")
        .withColumnRenamed("db_name", s"${prefix}_database")
    }
  }
}
