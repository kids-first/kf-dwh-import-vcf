package org.kidsfirstdrc.dwh.variantDbStats

import org.apache.spark.sql.SaveMode
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should.Matchers


class IndexVariantDbStatsSpec extends AnyFeatureSpec with GivenWhenThen with WithSparkSession with Matchers {

  Feature("Run") {
    Scenario("Get variant stats") {
      withOutputFolder("output") { output =>

        spark.sql("create database if not exists variant")
        spark.sql("use variant")
        Given("3 tables")

        val table1 = spark
          .read
          .json(getClass.getResource("/tables/occurrences/occurrences_sd_6789.json").getFile)
        table1.write.mode(SaveMode.Overwrite)
          .option("path", s"$output/occurrences_sd_6789")
          .format("json")
          .saveAsTable("variant.occurrences_sd_6789")

        val table2 = spark
          .read
          .json(getClass.getResource("/tables/occurrences/occurrences_sd_12345.json").getFile)
        table2.write.mode(SaveMode.Overwrite)
          .option("path", s"$output/occurrences_sd_12345")
          .format("json")
          .saveAsTable("variant.occurrences_sd_12345")

        val table3 = spark
          .read
          .json(getClass.getResource("/tables/occurrences/occurrences_sd_12345_re_00012.json").getFile)
        table3.write.mode(SaveMode.Overwrite)
          .option("path", s"$output/occurrences_sd_12345_re_00012")
          .format("json")
          .saveAsTable("variant.occurrences_sd_12345_re_00012")

        val occurrencesList = StatsUtils.getOccurrencesTableWORelease("variant")

        occurrencesList shouldBe Seq("occurrences_sd_12345", "occurrences_sd_6789")


        val allOccurrencesList = StatsUtils.getUnionOfOccurrences("variant", occurrencesList)

        allOccurrencesList.collect() should contain theSameElementsAs table1.union(table2).collect()

        val stats = StatsUtils.getStats(allOccurrencesList)
        val expectedStats = VariantDbStats(2,5,4,5)

        stats should equal (expectedStats)

      }
    }
  }


}