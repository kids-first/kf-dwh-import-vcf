package org.kidsfirstdrc.dwh.vcf

import bio.ferlab.datalake.spark3.config.{Configuration, DatasetConf, StorageConf}
import org.apache.spark.sql.functions.{array_sort, col, explode, lit}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.kidsfirstdrc.dwh.conf.Catalog.{DataService, HarmonizedData}
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.kidsfirstdrc.dwh.testutils.dataservice._
import org.kidsfirstdrc.dwh.testutils.vcf.{OccurrenceOutput, PostCGPInput}
import bio.ferlab.datalake.spark3.implicits.SparkUtils.columns.annotations
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class OccurrencesFamilySpec
    extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {

  import spark.implicits._
  spark.sparkContext.setCheckpointDir(getClass.getClassLoader.getResource(".").getFile)

  val studyId      = "SD_123456"
  val releaseId    = "RE_ABCDEF"
  val releaseId_lc = releaseId.toLowerCase

  implicit val conf: Configuration =
    Configuration(
      List(StorageConf("kf-strides-variant", getClass.getClassLoader.getResource(".").getFile))
    )

  val biospecimensDf = Seq(
    BiospecimenOutput(
      biospecimen_id = "BS_HIJKKL",
      participant_id = "PT_000001",
      family_id = "FA_000001",
      study_id = "SD_123456"
    ),
    BiospecimenOutput(
      biospecimen_id = "BS_HIJKKL2",
      participant_id = "PT_000002",
      family_id = "FA_000001",
      study_id = "SD_123456"
    ),
    BiospecimenOutput(
      biospecimen_id = "BS_2CZNEQQ5",
      participant_id = "PT_000003",
      family_id = "FA_000001",
      study_id = "SD_123456"
    ),
    BiospecimenOutput(
      biospecimen_id = "BS_ABCD1234",
      participant_id = "PT_000001",
      family_id = "FA_000001",
      study_id = "SD_123456"
    ),
    BiospecimenOutput(
      biospecimen_id = "BS_EFGH4567",
      participant_id = "PT_000002",
      family_id = "FA_000001",
      study_id = "SD_123456"
    ),
    BiospecimenOutput(
      biospecimen_id = "BS_IJKL8901",
      participant_id = "PT_000003",
      family_id = "FA_000001",
      study_id = "SD_123456"
    ),
    BiospecimenOutput(
      biospecimen_id = "BS_HIJKKL",
      participant_id = "PT_000001",
      family_id = "FA_000002",
      study_id = "SD_789"
    ),
    BiospecimenOutput(
      biospecimen_id = "BS_HIJKKL2",
      participant_id = "PT_000002",
      family_id = "FA_000002",
      study_id = "SD_789"
    ),
    BiospecimenOutput(
      biospecimen_id = "BS_2CZNEQQ5",
      participant_id = "PT_000003",
      family_id = "FA_000002",
      study_id = "SD_789"
    )
  ).toDF()

  val genomic_filesDf = Seq(
    GenomicFileOutput(
      acl = List("*"),
      file_name = "sample.CGP.filtered.deNovo.vep.vcf.gz",
      study_id = "SD_123456"
    )
  ).toDF()

  val genomic_files_overrideDf = Seq(
    GenomicFileOverrideOutput(
      file_name = "sample.CGP.filtered.deNovo.vep.vcf.gz",
      study_id = "SD_123456"
    )
  ).toDF

  val participantsDf = Seq(
    ParticipantOutput(
      kf_id = "PT_000001",
      affected_status = true,
      is_proband = true,
      study_id = "SD_123456",
      gender = "Male"
    ),
    ParticipantOutput(
      kf_id = "PT_000002",
      affected_status = true,
      is_proband = true,
      study_id = "SD_123456",
      gender = "Female"
    ),
    ParticipantOutput(
      kf_id = "PT_000003",
      affected_status = true,
      is_proband = true,
      study_id = "SD_123456",
      gender = "Male"
    )
  ).toDF

  val family_relationshipsDf = Seq(
    FamilyRelationshipOutput(
      kf_id = "FR_000001",
      participant2 = "PT_000001",
      participant1 = "PT_000002",
      participant1_to_participant2_relation = "Mother",
      study_id = "SD_123456"
    ),
    FamilyRelationshipOutput(
      kf_id = "FR_000003",
      participant2 = "PT_000001",
      participant1 = "PT_000003",
      participant1_to_participant2_relation = "Father",
      study_id = "SD_123456"
    ),
    FamilyRelationshipOutput(
      kf_id = "FR_000002",
      participant2 = "PT_000002",
      participant1 = "PT_000001",
      participant1_to_participant2_relation = "Child",
      study_id = "SD_123456"
    ),
    FamilyRelationshipOutput(
      kf_id = "FR_000004",
      participant2 = "PT_000003",
      participant1 = "PT_000001",
      participant1_to_participant2_relation = "Child",
      study_id = "SD_123456"
    )
  ).toDF

  "transform" should "return a dataframe with all expected columns" in {
    spark.sql("CREATE DATABASE IF NOT EXISTS variant")
    spark.sql("use variant")

    val postCGP = Seq(
      PostCGPInput()
    ).toDF()
      .withColumn("file_name", lit("file_1"))
      .withColumn("annotation", annotations)
      .withColumn("hgvsg", array_sort(col("annotation.HGVSg"))(0))
      .withColumn("variant_class", array_sort(col("annotation.VARIANT_CLASS"))(0))
      .drop("annotation", "INFO_ANN")
      .withColumn("genotype", explode($"genotypes"))

    val inputData = Map(
      DataService.participants.id           -> participantsDf.where($"study_id" === studyId),
      DataService.biospecimens.id           -> biospecimensDf.where($"study_id" === studyId),
      DataService.family_relationships.id   -> family_relationshipsDf.where($"study_id" === studyId),
      HarmonizedData.family_variants_vcf.id -> postCGP
    )

    val outputDf = new OccurrencesFamily(
      studyId,
      releaseId,
      "",
      "biospecimen_id",
      ".CGP.filtered.deNovo.vep.vcf.gz",
      ".postCGP.filtered.deNovo.vep.vcf.gz"
    ).transform(inputData)

    outputDf.as[OccurrenceOutput].collect() should contain theSameElementsAs Seq(
      OccurrenceOutput(participant_id = "PT_000002",
        biospecimen_id = "BS_HIJKKL2",
        `calls` = List(0, 0),
        `zygosity` = "WT",
        `has_alt` = 0,
        `gender` = "Female",
        `is_multi_allelic` = true,
        `transmission` = Some("non carrier proband"),
        `hgvsg` = null,
        `variant_class` = null),
      OccurrenceOutput(participant_id = "PT_000003",
        biospecimen_id = "BS_2CZNEQQ5",
        `calls` = List(1, -1),
        `zygosity` = "UNK",
        `has_alt` = 1,
        `is_multi_allelic` = true,
        `gender` = "Male",
        `hgvsg` = null,
        `variant_class` = null),
      OccurrenceOutput(
        `participant_id` = "PT_000001",
        `biospecimen_id` = "BS_HIJKKL",
        `mother_id` = Some("PT_000002"),
        `father_id` = Some("PT_000003"),
        `mother_calls` = Some(List(0, 0)),
        `father_calls` = Some(List(1, -1)),
        `calls` = List(1, 0),
        `is_multi_allelic` = true,
        `parental_origin` = Some("father"),
        `mother_zygosity` = Some("WT"),
        `father_zygosity` = Some("UNK"),
        `zygosity` = "HET",
        `has_alt` = 1,
        `gender` = "Male",
        `mother_affected_status` = Some(true),
        `father_affected_status` = Some(true),
        `hgvsg` = null,
        `variant_class` = null
      )
    )
  }

  "run" should "return a dataframe with all expected columns" in {
    spark.sql("CREATE DATABASE IF NOT EXISTS variant")
    loadTestData(DataService.biospecimens, biospecimensDf, "biospecimens", releaseId_lc)
    loadTestData(DataService.genomic_files, genomic_filesDf, "genomic_files", releaseId_lc)
    loadTestData(
      DataService.genomic_files_override,
      genomic_files_overrideDf,
      "genomic_files_override"
    )
    loadTestData(DataService.participants, participantsDf, "participants", releaseId_lc)
    loadTestData(
      DataService.family_relationships,
      family_relationshipsDf,
      "family_relationships",
      releaseId_lc
    )

    val input = getClass.getResource("/input_vcf/SD_123456").getFile

    spark.sql("use variant")

    val outputDf = new OccurrencesFamily(
      studyId,
      releaseId,
      input,
      "biospecimen_id",
      ".CGP.filtered.deNovo.vep.vcf.gz",
      ".postCGP.filtered.deNovo.vep.vcf.gz",
      None
    ).run()

    outputDf.select(
      "chromosome", "start", "reference", "alternate",
      "participant_id", "zygosity", "mother_calls", "father_calls", "parental_origin").show(false)

    outputDf.as[OccurrenceOutput].count shouldBe 12
  }

  private def loadTestData(
      ds: DatasetConf,
      df: DataFrame,
      tableName: String,
      releaseId: String
  ): Unit = {
    spark.sql(s"drop table if exists variant.${tableName}_$releaseId ")
    df.write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${ds.rootPath}/dataservice/${tableName}/${tableName}_$releaseId")
      .saveAsTable(s"variant.${tableName}_$releaseId")
  }

  private def loadTestData(ds: DatasetConf, df: DataFrame, tableName: String): Unit = {
    spark.sql(s"drop table if exists variant.${tableName} ")
    df.write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${ds.rootPath}/${tableName}/$tableName")
      .saveAsTable(s"variant.${tableName}")
  }
}
