package org.kidsfirstdrc.dwh.vcf

import io.projectglow.Glow
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should.Matchers


class MultiFeatureSpec extends AnyFeatureSpec with GivenWhenThen with WithSparkSession with Matchers {

  Feature("Multi") {
    Scenario("Transform vcf with vep") {
      val input = getClass.getResource("/multi.vcf").getFile

      val df = spark.read.format("vcf")
        .option("flattenInfoFields", "true").load(input)
      df.show(false)

      val output = Glow.transform("split_multiallelics", df.withColumnRenamed("filters", "INFO_FILTERS"))
      output.show(false)
    }
  }


}

