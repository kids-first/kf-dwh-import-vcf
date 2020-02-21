package org.kidsfirstdrc.dwh.vcf

import io.projectglow.Glow
import org.kidsfirstdrc.dwh.vcf.util.WithSparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should.Matchers


class MultiFeatureSpec extends AnyFeatureSpec with GivenWhenThen with WithSparkSession with Matchers {

  Feature("Multi") {
    Scenario("Transform vcf with vep") {
      val input = getClass.getResource("/multi.vcf").getFile

      val df = spark.read.format("com.databricks.vcf").load(input).coalesce(1)

      val output = Glow.transform("normalize_variants", df, Map(
        "mode" -> "split"
      ))
      output.show(false)
    }
  }


}

