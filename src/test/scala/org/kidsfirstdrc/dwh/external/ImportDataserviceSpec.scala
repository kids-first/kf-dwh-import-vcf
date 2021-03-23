package org.kidsfirstdrc.dwh.external

import org.apache.spark.sql.SaveMode
import org.kidsfirstdrc.dwh.testutils.Model.{BiosepecimenOutput, BiospecimenInput, ParticipantOutput}
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class ImportDataserviceSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {


  import spark.implicits._

  val releaseId = "RE_ABCDEF"
  "parseArgs" should "parse all args" in {
    ImportDataservice.parseArgs(Array("study1", "release1", "input", "output", "true")) shouldBe(Set("study1"), "release1", "input", "output", true, Dataservice.ALL_TABLES)
    ImportDataservice.parseArgs(Array("study1", "release1", "input", "output", "false")) shouldBe(Set("study1"), "release1", "input", "output", false, Dataservice.ALL_TABLES)
    ImportDataservice.parseArgs(Array("study1,study2", "release1", "input", "output", "true")) shouldBe(Set("study1", "study2"), "release1", "input", "output", true, Dataservice.ALL_TABLES)
    ImportDataservice.parseArgs(Array("study1", "release1", "input", "output", "false", "participants,biospecimens")) shouldBe(Set("study1"), "release1", "input", "output", false, Set("participants", "biospecimens"))
    ImportDataservice.parseArgs(Array("study1", "release1", "input", "output", "false", "all")) shouldBe(Set("study1"), "release1", "input", "output", false, Dataservice.ALL_TABLES)
  }

  "build" should "produce new tables when mergeExisting is false" in {
    val (studyId1, studyId2) = ("SD_123", "SD_456")

    withOutputFolder("dataservice") { workDir =>
      spark.sql("create database if not exists variant")
      spark.sql("use variant")
      val input = s"$workDir/raw"
      val output = s"$workDir/output"
      Given("Parquet files for participants of study 1")
      Seq(
        ParticipantOutput(kf_id = "PT_001"),
        ParticipantOutput(kf_id = "PT_002")
      ).toDF().write.parquet(s"$input/participants/$studyId1")

      Given("Parquet files for participants of study 2")
      Seq(
        ParticipantOutput(kf_id = "PT_003"),
        ParticipantOutput(kf_id = "PT_004")
      ).toDF().write.parquet(s"$input/participants/$studyId2")

      Given("Parquet files for participants of a study that is not include in the release")
      Seq(
        ParticipantOutput(kf_id = "PT_999")
      ).toDF().write.parquet(s"$input/participants/ignore_studies")

      Given("Parquet files for biospecimens of study 1")
      Seq(
        BiospecimenInput(),
        BiospecimenInput(kf_id = "BS_002", participant_id = "PT_002")
      ).toDF().write.parquet(s"$input/biospecimens/$studyId1")
      Given("Parquet files for biospecimens of study 2")
      Seq(
        BiospecimenInput(kf_id = "BS_003", participant_id = "PT_003"),
        BiospecimenInput(kf_id = "BS_004", participant_id = "unknown_participant")
      ).toDF().write.parquet(s"$input/biospecimens/$studyId2")

      When("Building the tables for studies 1 and 2, without merge with existing")
      ImportDataservice.build(Set(studyId1, studyId2), releaseId, input, output, mergeExisting = false, Set("participants", "biospecimens"))

      val participantsResult = spark.table(s"variant.participants_${releaseId.toLowerCase}").as[ParticipantOutput]
      val biospecimensResult = spark.table(s"variant.biospecimens_${releaseId.toLowerCase}").as[BiosepecimenOutput]

      Then("Participants table is created for this release, and this table contains all participants of both studies")
      participantsResult.collect() should contain theSameElementsAs Seq(
        ParticipantOutput(),
        ParticipantOutput(kf_id = "PT_002"),
        ParticipantOutput(kf_id = "PT_003", study_id = studyId2),
        ParticipantOutput(kf_id = "PT_004", study_id = studyId2)
      )

      Then("Biospecimens table is created for this release, and this table contains all biospecimens of both studies that are associate to an existing participants")
      biospecimensResult.collect() should contain theSameElementsAs Seq(
        BiosepecimenOutput(),
        BiosepecimenOutput(kf_id = "BS_002", biospecimen_id = "BS_002", participant_id = "PT_002"),
        BiosepecimenOutput(kf_id = "BS_003", biospecimen_id = "BS_003", participant_id = "PT_003", study_id = studyId2)
      )


    }
  }

  it should "produce new tables merged with existing" in {
    val studyId1 = "SD_123"
    withOutputFolder("dataservice") { workDir =>
      spark.sql("create database if not exists variant")
      spark.sql("use variant")
      val input = s"$workDir/raw"
      val output = s"$workDir/output"
      Given("Parquet files for participants of study 1")
      Seq(
        ParticipantOutput(kf_id = "PT_001"),
        ParticipantOutput(kf_id = "PT_002")
      ).toDF().write.parquet(s"$input/participants/$studyId1")

      Given("Parquet files for participants of a study that is not include in the release")
      Seq(
        ParticipantOutput(kf_id = "PT_999")
      ).toDF().write.parquet(s"$input/participants/ignore_studies")

      Given("A table participant that already exist")
      val existingStudy = "SD_456"
      val existingRelease = "previous_release"
      Seq(
        ParticipantOutput(kf_id = "removed_participant_from_study1"),
        ParticipantOutput(kf_id = "PT_003", study_id = existingStudy, release_id = existingRelease)
      ).toDF().write.mode(SaveMode.Overwrite)
        .option("path", s"$output/participants")
        .format("parquet")
        .saveAsTable("participants")

      When("Building the tables for studies 1, with merge with existing")
      ImportDataservice.build(Set(studyId1), releaseId, input, output, mergeExisting = true, Set("participants"))

      val participantsResult = spark.table(s"variant.participants_${releaseId.toLowerCase}").as[ParticipantOutput]

      Then("Participants table is created for this release, and this table contains all participants of both studies")
      participantsResult.collect() should contain theSameElementsAs Seq(
        ParticipantOutput(),
        ParticipantOutput(kf_id = "PT_002"),
        ParticipantOutput(kf_id = "PT_003", study_id = existingStudy, release_id = existingRelease)
      )

    }
  }
}
