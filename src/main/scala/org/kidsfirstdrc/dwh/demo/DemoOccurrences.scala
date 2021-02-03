package org.kidsfirstdrc.dwh.demo

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.kidsfirstdrc.dwh.utils.SparkUtils._
import org.kidsfirstdrc.dwh.vcf.Occurrences

object DemoOccurrences {

  def run(studyId: String, releaseId: String, input: String, output: String)(implicit spark: SparkSession): Unit = {
    Occurrences.write(build(studyId, releaseId, input), output, studyId, releaseId)
  }

  def build(studyId: String, releaseId: String, input: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val inputDF = vcf(input)
      .withColumn("genotype", explode(col("genotypes")))
      .withColumn("file_name", regexp_extract(input_file_name(), ".*/(.*)", 1))

    val occurrences = Occurrences.selectOccurrences(studyId, releaseId, inputDF)
      .withColumn("participant_id", col("biospecimen_id"))
      .withColumn("is_gru", lit(null).cast(BooleanType))
      .withColumn("is_hmb", lit(null).cast(BooleanType))
      .withColumn("is_proband", lit(null).cast(BooleanType))
      .withColumn("affected_status", lit(null).cast(BooleanType))
      .withColumn("dbgap_consent_code", lit(null).cast(StringType))

    val relations = broadcast(spark.read.option("sep", "\t")
      .option("header", "true")
      .csv("s3a://kf-strides-variant-parquet-prd/raw/1000Genomes/20130606_g1k.ped")
      .withColumn("participant_id", col("Individual ID"))
      .select(
        when($"Family ID" === 0, lit(null).cast("string")).otherwise($"Family ID") as "family_id",
        $"Individual ID" as "participant_id",
        when($"Maternal ID" === 0, lit(null).cast("string")).otherwise($"Maternal ID") as "mother_id",
        when($"Paternal ID" === 0, lit(null).cast("string")).otherwise($"Paternal ID") as "father_id",
        $"Population" as "ethnicity"
      )
    )

    Occurrences.joinOccurrencesWithInheritance(occurrences, relations)

  }

}
