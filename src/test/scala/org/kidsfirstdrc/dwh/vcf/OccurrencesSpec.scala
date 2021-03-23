package org.kidsfirstdrc.dwh.vcf

import bio.ferlab.datalake.core.config.Configuration
import org.apache.spark.sql.functions.{explode, lit}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.kidsfirstdrc.dwh.testutils.dataservice._
import org.kidsfirstdrc.dwh.testutils.vcf.{OccurrenceOutput, PostCGPInput}
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class OccurrencesSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {

  import spark.implicits._

  val studyId = "SD_123456"
  val releaseId = "RE_ABCDEF"
  val releaseId_lc = releaseId.toLowerCase

  implicit val conf: Configuration =  Configuration(List())

  val biospecimensDf = Seq(
    BiospecimenOutput(biospecimen_id = "BS_HIJKKL"  , participant_id = "PT_000001", family_id = "FA_000001", study_id = "SD_123456"),
    BiospecimenOutput(biospecimen_id = "BS_HIJKKL2" , participant_id = "PT_000002", family_id = "FA_000001", study_id = "SD_123456"),
    BiospecimenOutput(biospecimen_id = "BS_2CZNEQQ5", participant_id = "PT_000003", family_id = "FA_000001", study_id = "SD_123456"),
    BiospecimenOutput(biospecimen_id = "BS_HIJKKL"  , participant_id = "PT_000001", family_id = "FA_000002", study_id = "SD_789"),
    BiospecimenOutput(biospecimen_id = "BS_HIJKKL2" , participant_id = "PT_000002", family_id = "FA_000002", study_id = "SD_789"),
    BiospecimenOutput(biospecimen_id = "BS_2CZNEQQ5", participant_id = "PT_000003", family_id = "FA_000002", study_id = "SD_789")
  ).toDF()

  val genomic_filesDf = Seq(
    GenomicFileOutput(acl = List("*"), file_name = "sample.CGP.filtered.deNovo.vep.vcf.gz", study_id = "SD_123456")
  ).toDF()

  val genomic_files_overrideDf = Seq(
    GenomicFileOverrideOutput(file_name = "sample.CGP.filtered.deNovo.vep.vcf.gz", study_id = "SD_123456")
  ).toDF

  val participantsDf = Seq(
    ParticipantOutput(kf_id = "PT_000001", affected_status = true, is_proband = true, study_id = "SD_123456"),
    ParticipantOutput(kf_id = "PT_000002", affected_status = true, is_proband = true, study_id = "SD_123456")
  ).toDF

  val family_relationshipsDf = Seq(
    FamilyRelationshipOutput(kf_id = "FR_000001", participant2 = "PT_000001", participant1 = "PT_000002", participant1_to_participant2_relation = "Mother", study_id = "SD_123456"),
    FamilyRelationshipOutput(kf_id = "FR_000002", participant2 = "PT_000002", participant1 = "PT_000001", participant1_to_participant2_relation = "Child" , study_id = "SD_123456")
  ).toDF

  "build" should "return a dataframe with all expected columns" in {
    spark.sql("CREATE DATABASE IF NOT EXISTS variant")
    val output: String = getClass.getClassLoader.getResource(".").getFile
    loadTestData(output, biospecimensDf, "biospecimens", releaseId_lc)
    loadTestData(output, genomic_filesDf, "genomic_files", releaseId_lc)
    loadTestData(output, genomic_files_overrideDf, "genomic_files_override")
    loadTestData(output, participantsDf, "participants", releaseId_lc)
    loadTestData(output, family_relationshipsDf, "family_relationships", releaseId_lc)

    spark.sql("use variant")

    val postCGP = Seq(
      PostCGPInput()
    ).toDF()
      .withColumn("file_name", lit("file_1"))
      .withColumn("genotype", explode($"genotypes"))

    val outputDf = new Occurrences(studyId, releaseId, "", "" ).build(studyId, releaseId, postCGP, "biospecimen_id")
    outputDf.show(false)

    outputDf.as[OccurrenceOutput].collect() should contain theSameElementsAs Seq(
      OccurrenceOutput(participant_id = "PT_000002", biospecimen_id = "BS_HIJKKL2"),
      OccurrenceOutput(participant_id = "PT_000001", biospecimen_id = "BS_HIJKKL", `mother_id` = Some("PT_000002"), mother_calls = Some(List(0, 0)), mother_zygosity = Some("WT"))
    )

  }

  private def loadTestData(output: String, df: DataFrame, tableName: String, releaseId: String): Unit = {
    spark.sql(s"drop table if exists variant.${tableName}_$releaseId ")
    df.write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"$output/${tableName}_$releaseId")
      .saveAsTable(s"variant.${tableName}_$releaseId")
  }

  private def loadTestData(output: String, df: DataFrame, tableName: String): Unit = {
    spark.sql(s"drop table if exists variant.${tableName} ")
    df.write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"$output/$tableName")
      .saveAsTable(s"variant.${tableName}")
  }
}
