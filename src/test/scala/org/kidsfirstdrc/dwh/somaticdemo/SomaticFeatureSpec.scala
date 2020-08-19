package org.kidsfirstdrc.dwh.somaticdemo

import org.apache.spark.sql.SaveMode
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.kidsfirstdrc.dwh.utils.SparkUtils.vcf
import org.scalatest.GivenWhenThen
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should.Matchers


class SomaticFeatureSpec extends AnyFeatureSpec with GivenWhenThen with WithSparkSession with Matchers {

  Feature("Multi") {
    Scenario("Transform vcf with vep") {
      Given("A study id SD_123456")
      val studyId = "SD_123456"

      And("A release id RE_ABCDEF")
      val releaseId = "RE_ABCDEF"
      val input = getClass.getResource("/somatic_demo/vcf/mutect2.vcf.gz").getFile
      withOutputFolder("output") { output =>
        And("A table biospecimens_re_abcdef")
        spark.sql("create database if not exists variant")
        spark.sql("drop table if exists variant.biospecimens_sd_re_abcdef ")

        val bioDF = spark
          .read
          .json(getClass.getResource("/somatic_demo/biospecimens").getFile)
        bioDF.write.mode(SaveMode.Overwrite)
          .option("path", s"$output/biospecimens_re_abcdef")
          .format("json")
          .saveAsTable("variant.biospecimens_re_abcdef")
        spark.sql("use variant")
        val df = Consequences.build(studyId, releaseId, vcf(input))
        df.printSchema()
        df.show(false)
      }
    }
  }


}

