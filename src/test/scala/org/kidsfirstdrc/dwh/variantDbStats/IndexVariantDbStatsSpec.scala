package org.kidsfirstdrc.dwh.variantDbStats

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class IndexVariantDbStatsSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {
  val table1: DataFrame = spark
    .read
    .json(getClass.getResource("/tables/occurrences/occurrences_sd_6789.json").getFile)

  val table2: DataFrame = spark
    .read
    .json(getClass.getResource("/tables/occurrences/occurrences_sd_12345.json").getFile)

  val table3: DataFrame = spark
    .read
    .json(getClass.getResource("/tables/occurrences/occurrences_sd_12345_re_00012.json").getFile)

  "StatsUtil" should "return only occurrences tables without release" in {
      withOutputFolder("output") { output =>

        spark.sql("create database if not exists variant")
        spark.sql("use variant")
        Given("3 tables")


        table1.write.mode(SaveMode.Overwrite)
          .option("path", s"$output/occurrences_sd_6789")
          .format("json")
          .saveAsTable("variant.occurrences_sd_6789")


        table2.write.mode(SaveMode.Overwrite)
          .option("path", s"$output/occurrences_sd_12345")
          .format("json")
          .saveAsTable("variant.occurrences_sd_12345")


        table3.write.mode(SaveMode.Overwrite)
          .option("path", s"$output/occurrences_sd_12345_re_00012")
          .format("json")
          .saveAsTable("variant.occurrences_sd_12345_re_00012")

        val occurrencesList = StatsUtils.getOccurrencesTableWORelease("variant")

        occurrencesList shouldBe Seq("occurrences_sd_12345", "occurrences_sd_6789")
      }
    }

  it should "create a union the input occurrences tables" in {
    withOutputFolder("output") { output =>

      spark.sql("create database if not exists variant")
      spark.sql("use variant")
      Given("3 tables")


      table1.write.mode(SaveMode.Overwrite)
        .option("path", s"$output/occurrences_sd_6789")
        .format("json")
        .saveAsTable("variant.occurrences_sd_6789")


      table2.write.mode(SaveMode.Overwrite)
        .option("path", s"$output/occurrences_sd_12345")
        .format("json")
        .saveAsTable("variant.occurrences_sd_12345")


      table3.write.mode(SaveMode.Overwrite)
        .option("path", s"$output/occurrences_sd_12345_re_00012")
        .format("json")
        .saveAsTable("variant.occurrences_sd_12345_re_00012")

      val inputOccurrencesList  = Array("occurrences_sd_12345", "occurrences_sd_6789")

      val allOccurrencesList = StatsUtils.getUnionOfOccurrences("variant", inputOccurrencesList)

      allOccurrencesList.collect() should contain theSameElementsAs table1.union(table2).collect()

    }
  }

  it should "get variant db stats from input dataframe" in {
    val inputDf = table1.union(table2)
    val stats = StatsUtils.getStats(inputDf)
    val expectedStats = VariantDbStats(2,5,4,5)

    stats should equal(expectedStats)
  }
}