package org.kidsfirstdrc.dwh.join

import org.apache.spark.sql.SaveMode
import org.kidsfirstdrc.dwh.testutils.Model._
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class JoinConsequencesSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {

  import spark.implicits._

  val releaseId = "RE_ABCDEF"

  "build" should "return a dataframe with all expected columns" in {
    withOutputFolder("output") { outputDir =>
      spark.sql("create database if not exists variant")
      spark.sql("use variant")
      Given("2 studies")
      val (studyId1, studyId2) = ("SD_123", "SD_456")

      Given("2 tables, one  for each study")
      //Study 1
      val csq1 = ConsequenceOutput(study_id = studyId1)

      Seq(csq1).toDF().write.mode(SaveMode.Overwrite)
        .option("path", s"$outputDir/consequences_sd_123_re_abcdef")
        .format("parquet")
        .saveAsTable("consequences_sd_123_re_abcdef")

      //Study 2
      val csq2 = ConsequenceOutput(chromosome = "3", start = 3000, end = 3000, "C", "A", name = Some("mutation_2"), hgvsg = Some("chr3:g.3000C>A"), cds_position = None, amino_acids = None, study_id = studyId2)
      val csq3 = csq1.copy(study_id = studyId2)

      Seq(csq2, csq3).toDF().write.mode(SaveMode.Overwrite)
        .option("path", s"$outputDir/consequences_sd_456_re_abcdef")
        .format("parquet")
        .saveAsTable("consequences_sd_456_re_abcdef")

      Given("1 existing table annotation that contains some data for at least one study")
      val studyId3 = "SD_789"
      val existingCsq1 = JoinConsequenceOutput(study_ids = Set(studyId3), release_id = "RE_PREVIOUS")
      val existingCsq2 = JoinConsequenceOutput(chromosome = "4", start = 4000, end = 4000, "C", "A", name = Some("mutation_3"), hgvsg = Some("chr4:g.4000C>A"), cds_position = None, amino_acids = None, coding_dna_change = None, aa_change = None, sift_score = None, study_ids = Set(studyId3), release_id = "RE_PREVIOUS")

      val removedOldCsq = JoinConsequenceOutput(alternate = "G", study_ids = Set(studyId1), release_id = "RE_PREVIOUS")

      Seq(existingCsq1, existingCsq2, removedOldCsq).toDF().write.mode(SaveMode.Overwrite)
        .option("path", s"$outputDir/consequences")
        .format("parquet")
        .saveAsTable("consequences")

      And("A table dbnsfp_scores exists")
      Seq(DBSNFPScore()).toDF().write.mode(SaveMode.Overwrite).option("path", s"$outputDir/dbnsfp_scores")
        .format("parquet")
        .saveAsTable("dbnsfp_scores")


      When("Join Consequences")
      JoinConsequences.join(Seq(studyId1, studyId2), releaseId, outputDir, mergeWithExisting = true)

      Then("A new table for the release is created")
      val variantReleaseTable = spark.table("variant.consequences_re_abcdef")

      And("this table should contain all merged data")
      val output = variantReleaseTable
        .as[JoinConsequenceOutput]
      val expectedOutput = Seq(
        JoinConsequenceOutput(study_ids = Set(studyId1, studyId2, studyId3)),
        JoinConsequenceOutput(
          chromosome = "3", start = 3000, end = 3000, "C", "A", name = Some("mutation_2"), hgvsg = Some("chr3:g.3000C>A"),
          cds_position = None, amino_acids = None, coding_dna_change = None, aa_change = None, sift_score = None,
          study_ids = Set(studyId2)),
        existingCsq2.copy(release_id = releaseId)

      )

      output.collect() should contain theSameElementsAs expectedOutput

    }
  }

}
