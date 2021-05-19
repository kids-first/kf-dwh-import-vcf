package org.kidsfirstdrc.dwh.vcf

import bio.ferlab.datalake.spark3.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETL
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.{Clinical, HarmonizedData, Public}
import org.kidsfirstdrc.dwh.utils.SparkUtils._
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns._

class Consequences(studyId: String, releaseId: String, input: String, cgp_pattern: String, post_cgp_pattern: String)
                  (implicit conf: Configuration)
  extends ETL(){

  val destination = Clinical.consequences

  override def extract()(implicit spark: SparkSession): Map[DatasetConf, DataFrame] = {
    val inputDF = vcf(
      (getVisibleFiles(input, studyId, releaseId, cgp_pattern) ++
      getVisibleFiles(input, studyId, releaseId, post_cgp_pattern)).distinct
    )
      .withColumn("file_name", filename)
      .select(chromosome, start, end, reference, alternate, name, annotations)

    Map(
      HarmonizedData.family_variants_vcf -> inputDF,
      Public.ensembl_mapping -> spark.table(s"${Public.ensembl_mapping.table.get.fullName}")
    )
  }

  override def transform(data: Map[DatasetConf, DataFrame])(implicit spark: SparkSession): DataFrame = {
    val ensembl_mappingDf = data(Public.ensembl_mapping)
      .select(
        col("ensembl_transcript_id"),
        col("is_canonical"),
        col("is_mane_plus") as "mane_plus",
        col("is_mane_select") as "mane_select",
        col("refseq_mrna_id"),
        col("refseq_protein_id"))

    val consequencesDf = data(HarmonizedData.family_variants_vcf)
      .groupBy(locus: _*)
      .agg(
        first("annotations") as "annotations",
        first("name") as "name",
        first("end") as "end"
      )
      .withColumn("annotation", explode(col("annotations")))
      .drop("annotations")
      .select(col("*"),
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
      .join(ensembl_mappingDf, Seq("ensembl_transcript_id"), "left")
      .withColumn("canonical", when(col("is_canonical").isNull, col("original_canonical")).otherwise(col("is_canonical")))
      .drop("is_canonical")
  }

  override def load(data: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val tableConsequences = tableName(destination.datasetid, studyId, releaseId)
    val salt = (rand * 3).cast(IntegerType) //3 files per chr, tried with 1 file per chr but got an OOM when writing parquet files
    data
      .repartition(69, col("chromosome"), salt)
      .write.mode(SaveMode.Overwrite)
      .partitionBy("study_id", "release_id", "chromosome")
      .format("parquet")
      .option("path", s"${destination.rootPath}/${destination.datasetid}/$tableConsequences")
      .saveAsTable(tableConsequences)
    data
  }

  override def run()(implicit spark: SparkSession): DataFrame = {
    val input = extract()

    val consequences = transform(input)

    load(consequences)
  }
}
