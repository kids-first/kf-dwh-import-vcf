package org.kidsfirstdrc.dwh.vcf

import io.projectglow.Glow
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.functions._

class NormalizeSpec extends AnyFeatureSpec with GivenWhenThen with WithSparkSession with Matchers {

  Feature("Normalize") {
    Scenario("Normalize and split") {

      val input = getClass.getResource("/test.vcf").getFile
      val fasta = getClass.getResource("/example.fasta").getFile

      import spark.implicits._
      val df = spark.read.format("com.databricks.vcf").load(input).coalesce(1)
      df.show(false)
//      df.withColumn("genotype", explode($"genotypes")).select("*", "genotype.*").show(false)
      val split = Glow.transform("split_multiallelics", df/*.withColumnRenamed("filters", "INFO_filters")*/)
      split.show(false)
      val norm = Glow.transform("normalize_variants", df, Map("reference_genome_path" -> fasta))
      norm
        .withColumn("genotype", explode($"genotypes")).select("*", "genotype.*")
        .show(truncate=false)

    }
  }


}

