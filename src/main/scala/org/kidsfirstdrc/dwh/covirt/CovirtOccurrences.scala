package org.kidsfirstdrc.dwh.covirt

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}
import org.kidsfirstdrc.dwh.utils.SparkUtils._
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns._

object CovirtOccurrences {

  def run(releaseId: String, input: String, output: String)(implicit spark: SparkSession): Unit = {
    write(build(input), output, releaseId)
  }

  def build(input: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val occurrences = vcf(input, None)
      .withColumn("genotype", explode($"genotypes"))
      .select(
        chromosome,
        start,
        end,
        reference,
        alternate,
        name,
        firstAnn,
        $"genotype.sampleId" as "vcf_sample_id",
        $"genotype.alleleDepths" as "ad",
        $"genotype.depth" as "dp",
        $"genotype.conditionalQuality" as "gq",
        $"genotype.calls" as "calls",
        array_contains($"genotype.calls", 1) as "has_alt",
        is_multi_allelic,
        old_multi_allelic
      )
      .withColumn("hgvsg", hgvsg)
      .withColumn("variant_class", variant_class)
      .drop("annotation")

    val manifest = broadcast(
      spark
        .table("manifest")
        .select($"vcf_sample_id", $"aliquot_id", $"sample_id", $"sample_type")
    )

    occurrences
      .join(manifest, occurrences("vcf_sample_id") === manifest("vcf_sample_id"), "inner")
      .drop(occurrences("vcf_sample_id"))
  }

  def write(df: DataFrame, output: String, releaseId: String)(implicit
      spark: SparkSession
  ): Unit = {
    import spark.implicits._
    val tableOccurence = s"occurences_${releaseId.toLowerCase}"
    df
      .repartition($"chromosome")
      .withColumn(
        "bucket",
        functions
          .ntile(8)
          .over(
            Window
              .partitionBy("chromosome")
              .orderBy("start")
          )
      )
      .repartition($"chromosome", $"bucket")
      .sortWithinPartitions($"chromosome", $"bucket", $"start")
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("chromosome")
      .format("parquet")
      .option("path", s"$output/occurrences/covirt/$tableOccurence")
      .saveAsTable(tableOccurence)
  }

}
