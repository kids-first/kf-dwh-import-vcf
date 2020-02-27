package org.kidsfirstdrc.dwh.vcf

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SaveMode
import org.kidsfirstdrc.dwh.testutils.Model._
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should.Matchers


class AppFeatureSpec extends AnyFeatureSpec with GivenWhenThen with WithSparkSession with Matchers {

  import spark.implicits._

  Feature("Run") {
    Scenario("Transform vcf into parquet files") {
      Given("A study id SD_123456")
      val studyId = "SD_123456"

      And("A release id RE_ABCDEF")
      val releaseId = "RE_ABCDEF"

      And("An input folder that contain VCF")
      val input = getClass.getResource("/input_vcf/SD_123456").getFile

      And("An empty output folder")
      val output = "tmp/output_test"
      FileUtils.deleteDirectory(new File(output))

      And("A table biospecimens_sd_123456_re_abcdef")
      spark.sql("create database if not exists variant")
      spark.sql("drop table if exists variant.biospecimens_sd_123456_re_abcdef ")

      val bioDF = spark
        .read
        .json(getClass.getResource("/tables/biospecimens_sd_123456_re_abcdef").getFile)
      bioDF.write.mode(SaveMode.Overwrite)
        .option("path", s"$output/biospecimens_sd_123456_re_abcdef")
        .format("json")
        .saveAsTable("variant.biospecimens_sd_123456_re_abcdef")

      When("Run the main application")
      ImportVcf.run(studyId, releaseId, input, output)

      Then("Table occurences_sd_123456_re_abcdef should contain rows for the given study and release")
      val occurences = spark.table("variant.occurences_sd_123456_re_abcdef")
        .select(
          "chromosome",
          "start",
          "end",
          "reference",
          "alternate",
          "name",
          "biospecimen_id",
          "participant_id",
          "family_id",
          "study_id",
          "release_id",
          "dbgap_consent_code")
        .as[OccurencesOutput]

      val expectedOccurences = Seq(
        OccurencesOutput("1", 10438, 10440, "AC", "A", Some("rs112766696"), "BS_ABCD1234", "PT_000001", Some("FA_000001"), studyId, releaseId, "1"),
        OccurencesOutput("1", 10438, 10440, "AC", "A", Some("rs112766696"), "BS_EFGH4567", "PT_000002", Some("FA_000001"), studyId, releaseId, "1"),
        OccurencesOutput("1", 10438, 10440, "AC", "A", Some("rs112766696"), "BS_IJKL8901", "PT_000003", Some("FA_000001"), studyId, releaseId, "1"),
        OccurencesOutput("1", 10559, 10560, "C", "G", None, "BS_ABCD1234", "PT_000001", Some("FA_000001"), studyId, releaseId, "1"),
        OccurencesOutput("1", 10559, 10560, "C", "G", None, "BS_EFGH4567", "PT_000002", Some("FA_000001"), studyId, releaseId, "1"),
        OccurencesOutput("1", 10559, 10560, "C", "G", None, "BS_IJKL8901", "PT_000003", Some("FA_000001"), studyId, releaseId, "1")
      )
      occurences.collect() should contain theSameElementsAs expectedOccurences

      And("Table annotations_sd_123456_re_abcdef should contain rows for the given study and release")
      val annotations = spark.table("variant.annotations_sd_123456_re_abcdef")
        .select(
          "chromosome",
          "start",
          "end",
          "reference",
          "alternate",
          "hgvsg"
        )
        .as[(String, Long, Long, String, String, String)]
      val expectedAnnotations = Seq(
        ("1", 10438, 10440, "AC", "A", "chr1:g.10443del"),
        ("1", 10559, 10560, "C", "G", "chr1:g.10560C>G")
      )
      annotations.collect() should contain theSameElementsAs expectedAnnotations

      And("Table consequences_sd_123456_re_abcdef should contain rows for the given study and release")
      val consequences = spark.table("variant.consequences_sd_123456_re_abcdef")
        .select(
          "chromosome",
          "start",
          "end",
          "reference",
          "alternate",
          "symbol"
        ).as[(String, Long, Long, String, String, String)]
      val expectedConsequences = Seq(
        ("1", 10438, 10440, "AC", "A", "DDX11L1"),
        ("1", 10438, 10440, "AC", "A", "WASH7P"),
        ("1", 10559, 10560, "C", "G", "DDX11L1"),
        ("1", 10559, 10560, "C", "G", "WASH7P")
      )
      consequences.collect() should contain theSameElementsAs expectedConsequences

    }
  }




}

