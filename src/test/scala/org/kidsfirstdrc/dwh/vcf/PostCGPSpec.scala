package org.kidsfirstdrc.dwh.vcf

import io.projectglow.Glow
import org.apache.spark.sql.functions._
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should.Matchers

class PostCGPSpec extends AnyFeatureSpec with GivenWhenThen with WithSparkSession with Matchers {

  Feature("Normalize") {
    Scenario("Normalize and split") {

      val inputFolder = getClass.getResource("/postCGPMixed").getFile
      val input = s"$inputFolder/*.vcf"
      val fasta = getClass.getResource("/example.fasta").getFile

      import spark.implicits._
      val df = spark.read.format("com.databricks.vcf").option("flattenInfoFields", true).load(input).coalesce(1)
      df.show(false)
//      df.withColumn("genotype", explode($"genotypes")).select("*", "genotype.*").show(false)
      val split = Glow.transform("split_multiallelics", df/*.withColumnRenamed("filters", "INFO_FILTERS")*/)
      split.show(false)
//      val norm = Glow.transform("normalize_variants", split, Map("reference_genome_path" -> fasta))
//      norm
//        .withColumn("genotype", explode($"genotypes")).select("*", "genotype.*")
//        .show(false)
    }
  }


}

