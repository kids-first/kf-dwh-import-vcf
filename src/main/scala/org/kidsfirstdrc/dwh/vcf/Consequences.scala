package org.kidsfirstdrc.dwh.vcf

import bio.ferlab.datalake.core.config.Configuration
import bio.ferlab.datalake.core.etl.{DataSource, ETL}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.{Clinical, HarmonizedData}
import org.kidsfirstdrc.dwh.utils.SparkUtils._
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns._

class Consequences(studyId: String, releaseId: String, input: String)
                  (implicit conf: Configuration)
  extends ETL(Clinical.consequences){

  override def extract()(implicit spark: SparkSession): Map[DataSource, DataFrame] = {
    val inputDF = visibleVcf(allFilesPath(input), studyId, releaseId)
      .select(chromosome, start, end, reference, alternate, name, annotations)
    Map(
      HarmonizedData.family_variants_vcf -> inputDF
    )
  }

  override def transform(data: Map[DataSource, DataFrame])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    data(HarmonizedData.family_variants_vcf)
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
  }

  override def load(data: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val tableConsequences = tableName(destination.name, studyId, releaseId)
    val salt = (rand * 3).cast(IntegerType) //3 files per chr, tried with 1 file per chr but got an OOM when writing parquet files
    data
      .repartition(69, col("chromosome"), salt)
      .write.mode(SaveMode.Overwrite)
      .partitionBy("study_id", "release_id", "chromosome")
      .format("parquet")
      .option("path", s"${destination.rootPath}/${destination.name}/$tableConsequences")
      .saveAsTable(tableConsequences)
    data
  }

  override def run()(implicit spark: SparkSession): DataFrame = {
    val input = extract()

    val consequences = transform(input)

    load(consequences)
  }
}
