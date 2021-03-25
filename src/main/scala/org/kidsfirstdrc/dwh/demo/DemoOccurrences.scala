package org.kidsfirstdrc.dwh.demo

import bio.ferlab.datalake.core.config.Configuration
import bio.ferlab.datalake.core.etl.{DataSource, ETL}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.{Clinical, DataService, HarmonizedData}
import org.kidsfirstdrc.dwh.utils.SparkUtils._
import org.kidsfirstdrc.dwh.vcf.Occurrences

class DemoOccurrences(studyId: String, releaseId: String, input: String)
                     (implicit conf: Configuration)
  extends ETL(Clinical.occurrences){

  override def extract()(implicit spark: SparkSession): Map[DataSource, DataFrame] = {
    val inputDF = vcf(input)
      .withColumn("genotype", explode(col("genotypes")))
      .withColumn("file_name", regexp_extract(input_file_name(), ".*/(.*)", 1))

    val relations = spark.read.option("sep", "\t")
      .option("header", "true")
      .csv("s3a://kf-strides-variant-parquet-prd/raw/1000Genomes/20130606_g1k.ped")

    Map(
      DataService.family_relationships -> relations,
      HarmonizedData.family_variants_vcf -> inputDF
    )
  }

  override def transform(data: Map[DataSource, DataFrame])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val occurenceJob = new Occurrences(studyId, releaseId, input, "biospecimen_id")

    val inputDF = data(HarmonizedData.family_variants_vcf)

    val occurrences = occurenceJob.selectOccurrences(studyId, releaseId, inputDF)
      .withColumn("participant_id", col("biospecimen_id"))
      .withColumn("is_gru", lit(null).cast(BooleanType))
      .withColumn("is_hmb", lit(null).cast(BooleanType))
      .withColumn("is_proband", lit(null).cast(BooleanType))
      .withColumn("affected_status", lit(null).cast(BooleanType))
      .withColumn("dbgap_consent_code", lit(null).cast(StringType))

    val relations = data(DataService.family_relationships)
      .withColumn("participant_id", col("Individual ID"))
      .select(
        when($"Family ID" === 0, lit(null).cast("string")).otherwise($"Family ID") as "family_id",
        $"Individual ID" as "participant_id",
        when($"Maternal ID" === 0, lit(null).cast("string")).otherwise($"Maternal ID") as "mother_id",
        when($"Paternal ID" === 0, lit(null).cast("string")).otherwise($"Paternal ID") as "father_id",
        $"Population" as "ethnicity"
      )

    occurenceJob.joinOccurrencesWithInheritance(occurrences, broadcast(relations))

  }

  override def run()(implicit spark: SparkSession): DataFrame = {
    val demoOccurrences = transform(extract())
    load(demoOccurrences)
  }

  override def load(data: DataFrame)(implicit spark: SparkSession): DataFrame = {
    new Occurrences(studyId, releaseId, input, "biospecimen_id").load(data)
  }
}
