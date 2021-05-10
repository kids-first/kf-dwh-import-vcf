package org.kidsfirstdrc.dwh.join

import bio.ferlab.datalake.core.config.{Configuration, StorageConf}
import org.apache.spark.sql.SaveMode
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.kidsfirstdrc.dwh.testutils.external.ImportScores
import org.kidsfirstdrc.dwh.testutils.join.JoinConsequenceOutput
import org.kidsfirstdrc.dwh.testutils.vcf.{ConsequenceOutput, Exon, RefAlt}
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class JoinConsequencesSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {

  import spark.implicits._

  val releaseId = "RE_ABCDEF"

  "build" should "return a dataframe with all expected columns" in {
    withOutputFolder("output") { outputDir =>

      implicit val conf: Configuration = Configuration(List(
        StorageConf("kf-strides-variant", outputDir)
      ))

      spark.sql("create database if not exists variant")
      spark.sql("use variant")
      Given("2 studies")
      val studyId1 = "SD_123"
      val studyId2 = "SD_456"
      val studyId3 = "SD_789"

      Given("2 tables, one  for each study")
      //Study 1
      val csq1 = ConsequenceOutput(study_id = studyId1)

      Seq(csq1).toDF().write.mode(SaveMode.Overwrite)
        .option("path", s"$outputDir/consequences_sd_123_re_abcdef")
        .format("parquet")
        .saveAsTable("consequences_sd_123_re_abcdef")

      //Study 2
      val csq3 = csq1.copy(study_id = studyId2)

      Seq(csq3).toDF().write.mode(SaveMode.Overwrite)
        .option("path", s"$outputDir/consequences_sd_456_re_abcdef")
        .format("parquet")
        .saveAsTable("consequences_sd_456_re_abcdef")

      Given("1 existing table annotation that contains some data for at least one study")

      val existingCsq1 = JoinConsequenceOutput(study_ids = List(studyId3), release_id = "RE_PREVIOUS")

      Seq(existingCsq1).toDF().write.mode(SaveMode.Overwrite)
        .option("path", s"$outputDir/consequences")
        .format("parquet")
        .saveAsTable("consequences")

      And("A table dbnsfp_scores exists")
      //Seq(DBNSFPScore()).toDF().write.mode(SaveMode.Overwrite).option("path", s"$outputDir/dbnsfp_original")
      Seq(ImportScores.Output()).toDF().write.mode(SaveMode.Overwrite).option("path", s"$outputDir/dbnsfp_original")
        .format("parquet")
        .saveAsTable("dbnsfp_original")

      When("Join Consequences")
      new JoinConsequences(Seq(studyId1, studyId2), releaseId, mergeWithExisting = true, "variant").run()

      Then("A new table for the release is created")
      val variantReleaseTable = spark.table("variant.consequences_re_abcdef")

      And("this table should contain all merged data")
      val output = variantReleaseTable
        .as[JoinConsequenceOutput]
      val expectedOutput = Seq(
        JoinConsequenceOutput(study_ids = List(studyId3), `release_id` = "RE_ABCDEF"),
        JoinConsequenceOutput("2",165310406,165310406,"G","A","ENST00000283256.10",None,"Transcript",List("missense_variant"),
          Some("rs1057520413"),"MODERATE","SCN2A","ENSG00000136531",1,"protein_coding","SNV",Some(Exon(Some(7),Some(27))),None,
          "ENST00000283256.10:c.781G>A","ENSP00000283256.6:p.Val261Met",Some("chr2:g.166166916G>A"),Some(781),937,261,RefAlt("V","M"),
          RefAlt("GTG","ATG"), true, None, None, None, None, List("SD_456", "SD_123"),Some("V261M"),"781G>A","RE_ABCDEF",
          None,None,None,None,None,None,None,None,
          None,None,None,None,None,None,None,None,None,None,None,null,None,None,None,None,None,None,None,None,None,None,None,None,None,
          None,None,None,None,None,None,None,None,None,None,None,None,None,null,None,None,null,None,None,null,None,None,None,None,None,
          None,null,None,None,null,None,None,null,None,None,null,None,None,null,None,None,null,None,None,null,None,None,null,null,None,
          None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,
          null,None,None)

      )

      output.collect() should contain theSameElementsAs expectedOutput

    }
  }
}
