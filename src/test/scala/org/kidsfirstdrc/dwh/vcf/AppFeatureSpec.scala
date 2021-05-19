package org.kidsfirstdrc.dwh.vcf

import bio.ferlab.datalake.spark3.config.{Configuration, StorageConf}
import bio.ferlab.datalake.spark3.config.DatasetConf
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.kidsfirstdrc.dwh.conf.Catalog.DataService
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.kidsfirstdrc.dwh.testutils.external.EnsemblMappingOutput
import org.kidsfirstdrc.dwh.testutils.vcf.OccurrenceOutput
import org.scalatest.GivenWhenThen
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should.Matchers

class AppFeatureSpec extends AnyFeatureSpec with GivenWhenThen with WithSparkSession with Matchers {

  import spark.implicits._

  implicit val conf: Configuration =
    Configuration(List(StorageConf("kf-strides-variant", getClass.getClassLoader.getResource(".").getFile)))

  Feature("Run") {
    Scenario("Transform vcf into parquet files") {
      Given("A study id SD_123456")
      val studyId = "SD_123456"

      And("A release id RE_ABCDEF")
      val releaseId = "RE_ABCDEF"

      And("An input folder that contain VCF")
      val input = getClass.getResource("/input_vcf/SD_123456").getFile

      val inputEnsemblData = Seq(EnsemblMappingOutput()).toDF

      And("An empty output folder")
      withOutputFolder("output") { output =>
        spark.sql("create database if not exists variant")

        And("A table ensembl_mapping")
        inputEnsemblData
          .write.mode(SaveMode.Overwrite)
          .option("path", s"$output/ensembl_mapping")
          .format("parquet")
          .saveAsTable(s"variant.ensembl_mapping")

        And("A table biospecimens_re_abcdef")
        loadTestClinicalTable("biospecimens", s"${DataService.biospecimens.rootPath}/Dataservice")

        And("A table genomic_files_re_abcdef that contains only sample.CGP.filtered.deNovo.vep.vcf.gz")
        loadTestClinicalTable("genomic_files", s"${DataService.genomic_files.rootPath}/Dataservice")

        And("A table genomic_files_override that contains a file to include")
        loadTestTable("genomic_files_override", s"${DataService.genomic_files_override.rootPath}")

        And("A table participants_re_abcdef")
        loadTestClinicalTable("participants", s"${DataService.participants.rootPath}/Dataservice")

        And("A table family_relationships_re_abcdef")
        loadTestClinicalTable("family_relationships", s"${DataService.family_relationships.rootPath}/Dataservice")

        When("Run the main application")
        ImportVcf.run(studyId, releaseId, input)

        Then("Table occurrences_sd_123456_re_abcdef should contain rows for the given study and release")

        val occurrences = spark.table("variant.occurrences_sd_123456_re_abcdef").as[OccurrenceOutput]

        val expectedOccurrences = Seq(
          OccurrenceOutput("1", 10439, 10441, "AC", "A", Some("rs112766696"), "BS_ABCD1234", "PT_000001", "FA_000001", studyId, releaseId, "sample.CGP.filtered.deNovo.vep.vcf.gz", "c1"),
          OccurrenceOutput("1", 10439, 10441, "AC", "A", Some("rs112766696"), "BS_EFGH4567", "PT_000002", "FA_000001", studyId, releaseId, "sample.CGP.filtered.deNovo.vep.vcf.gz", "c1"),
          OccurrenceOutput("1", 10560, 10561, "C", "G" , None, "BS_ABCD1234", "PT_000001", "FA_000001", studyId, releaseId, "sample.CGP.filtered.deNovo.vep.vcf.gz", "c1"),
          OccurrenceOutput("1", 10560, 10561, "C", "G" , None, "BS_EFGH4567", "PT_000002", "FA_000001", studyId, releaseId, "sample.CGP.filtered.deNovo.vep.vcf.gz", "c1"),
          //MultiAllelic
          OccurrenceOutput("1", 15274, 15275, "A", "G" , None, "BS_ABCD1234", "PT_000001", "FA_000001", studyId, releaseId, "sample.CGP.filtered.deNovo.vep.vcf.gz", "c1"),
          OccurrenceOutput("1", 15274, 15275, "A", "G" , None, "BS_EFGH4567", "PT_000002", "FA_000001", studyId, releaseId, "sample.CGP.filtered.deNovo.vep.vcf.gz", "c1"),
          OccurrenceOutput("1", 15274, 15275, "A", "T" , None, "BS_ABCD1234", "PT_000001", "FA_000001", studyId, releaseId, "sample.CGP.filtered.deNovo.vep.vcf.gz", "c1"),
          OccurrenceOutput("1", 15274, 15275, "A", "T" , None, "BS_EFGH4567", "PT_000002", "FA_000001", studyId, releaseId, "sample.CGP.filtered.deNovo.vep.vcf.gz", "c1")
        )
        //occurrences.collect() should contain allElementsOf expectedOccurrences

        And("Table variants_sd_123456_re_abcdef should contain rows for the given study and release")
        val variants = spark.table("variant.variants_sd_123456_re_abcdef")
          .select(
            "chromosome",
            "start",
            "end",
            "reference",
            "alternate",
            "hgvsg"
          )
          .as[(String, Long, Long, String, String, String)]
        val expectedVariants = Seq(
          ("1", 10439, 10441, "AC", "A", "chr1:g.10443del"),
          ("1", 10560, 10561, "C", "G", "chr1:g.10560C>G"),
          ("1", 15274, 15275, "A", "G", "chr1:g.15274A>G"),
          ("1", 15274, 15275, "A", "T", "chr1:g.15274A>T")
        )
        variants.collect() should contain theSameElementsAs expectedVariants

        And("Table consequences_sd_123456_re_abcdef should contain rows for the given study and release")
        val consequences = spark.table("variant.consequences_sd_123456_re_abcdef")
          .select(
            "chromosome",
            "start",
            "end",
            "reference",
            "alternate",
            "symbol",
            "ensembl_transcript_id"
          ).as[(String, Long, Long, String, String, String, String)]

        val expectedConsequences = Seq(
          ("1", 10439L, 10441L, "AC", "A", "DDX11L1", "ENST00000450305"),
          ("1", 10439L, 10441L, "AC", "A", "DDX11L1", "ENST00000456328"),
          ("1", 10439L, 10441L, "AC", "A", "WASH7P", "ENST00000488147"),
          ("1", 10560L, 10561L, "C", "G", "DDX11L1", "ENST00000450305"),
          ("1", 10560L, 10561L, "C", "G", "DDX11L1", "ENST00000456328"),
          ("1", 10560L, 10561L, "C", "G", "WASH7P", "ENST00000488147"),
          ("1", 15274L, 15275L, "A", "G", "MIR6859-1", "ENST00000619216"),
          ("1", 15274L, 15275L, "A", "T", "MIR6859-1", "ENST00000619216")

        )

        consequences.collect() should contain theSameElementsAs expectedConsequences
      }
    }
  }


  private def loadTestClinicalTable(tableName: String, output: String): Unit = {
    spark.sql(s"drop table if exists variant.${tableName}_re_abcdef ")
    val genomicFilesDF = spark
      .read
      .json(this.getClass.getResource(s"/tables/${tableName}_re_abcdef").getFile)
    genomicFilesDF.write.mode(SaveMode.Overwrite)
      .option("path", s"$output/$tableName/${tableName}_re_abcdef")
      .format("parquet")
      .saveAsTable(s"variant.${tableName}_re_abcdef")
  }

  private def loadTestTable(tableName: String, output: String): Unit = {
    spark.sql(s"drop table if exists variant.${tableName}")
    val genomicFilesDF = spark
      .read
      .json(this.getClass.getResource(s"/tables/${tableName}").getFile)
    genomicFilesDF.write.mode(SaveMode.Overwrite)
      .option("path", s"$output/$tableName/${tableName}")
      .format("parquet")
      .saveAsTable(s"variant.${tableName}")
  }

}
