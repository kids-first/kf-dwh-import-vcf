package org.kidsfirstdrc.dwh.join

import org.apache.spark.sql.SaveMode
import org.kidsfirstdrc.dwh.testutils.Model._
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class JoinVariantsSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {

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
      val variant1 = VariantOutput(ac = 5, an = 10, af = 0.5, homozygotes = 2, heterozygotes = 3, study_id = studyId1)
      val variant2 = VariantOutput(chromosome = "3", start = 3000, end = 3000, reference = "T", alternate = "G", ac = 5, an = 20, af = 0.25, name = Some("mutation_2"), hgvsg = "chr3:g.2000T>G", homozygotes = 1, heterozygotes = 4, study_id = studyId1)

      Seq(variant1, variant2).toDF().write.mode(SaveMode.Overwrite)
        .option("path", s"$outputDir/variants_sd_123_re_abcdef")
        .format("parquet")
        .saveAsTable("variants_sd_123_re_abcdef")

      //Study 2
      val variant3 = VariantOutput(chromosome = "3", start = 3000, end = 3000, "C", "A", name = Some("mutation_2"), hgvsg = "chr3:g.2000T>G", ac = 10, an = 30, af = 0.33333333, homozygotes = 2, heterozygotes = 8, study_id = studyId2)
      val variant4 = variant1.copy(study_id = studyId2)

      Seq(variant3, variant4).toDF().write.mode(SaveMode.Overwrite)
        .option("path", s"$outputDir/variants_sd_456_re_abcdef")
        .format("parquet")
        .saveAsTable("variants_sd_456_re_abcdef")

      Given("1 existing table annotation that contains some data for at least one study")
      val studyId3 = "SD_789"
      val existingVariant1 = JoinVariantOutput(by_study = Map(
        studyId1 -> Freq(3, 2, 0.66666667, 1, 1),
        studyId3 -> Freq(7, 2, 0.5, 5, 1)
      ), release_id = "RE_PREVIOUS")

      val removedOldVariant = JoinVariantOutput(alternate= "G", by_study = Map(
        studyId1 -> Freq(100, 75, 0.75, 50, 25)
      ), release_id = "RE_PREVIOUS")

      val existingVariant2 = JoinVariantOutput(
        chromosome = "4", start = 4000, end = 4000, reference = "T", alternate = "G",
        ac = 2, an = 3, af = 0.66666667, homozygotes = 1, heterozygotes = 1,
        by_study = Map(
          studyId3 -> Freq(3, 2, 0.66666667, 1, 1)
        ),
        topmed = None, gnomad_genomes_2_1 = None, clinvar_id = None, clin_sig = None,
        release_id = "RE_PREVIOUS")

      Seq(existingVariant1, removedOldVariant, existingVariant2).toDF().write.mode(SaveMode.Overwrite)
        .option("path", s"$outputDir/variants")
        .format("parquet")
        .saveAsTable("variants")

      And("A table 1000_genomes exists")
      Seq(FrequencyEntry()).toDF().write.mode(SaveMode.Overwrite).option("path", s"$outputDir/1000_genomes")
        .format("parquet")
        .saveAsTable("1000_genomes")

      And("A table topmed_bravo exists")
      Seq(FrequencyEntry()).toDF().write.mode(SaveMode.Overwrite).option("path", s"$outputDir/topmed_bravo")
        .format("parquet")
        .saveAsTable("topmed_bravo")

      And("A table gnomad_genomes_2_1_1_liftover_grch38 exists")
      Seq(FrequencyEntry()).toDF().write.mode(SaveMode.Overwrite).option("path", s"$outputDir/gnomad_genomes_2_1_1_liftover_grch38")
        .format("parquet")
        .saveAsTable("gnomad_genomes_2_1_1_liftover_grch38")

      And("A table gnomad_exomes_2_1_1_liftover_grch38 exists")
      Seq(FrequencyEntry()).toDF().write.mode(SaveMode.Overwrite).option("path", s"$outputDir/gnomad_exomes_2_1_1_liftover_grch38")
        .format("parquet")
        .saveAsTable("gnomad_exomes_2_1_1_liftover_grch38")

      And("A table gnomad_genomes_3_0 exists")
      Seq(FrequencyEntry()).toDF().write.mode(SaveMode.Overwrite).option("path", s"$outputDir/gnomad_genomes_3_0")
        .format("parquet")
        .saveAsTable("gnomad_genomes_3_0")

      And("A table clinvar exists")
      Seq(ClinvarEntry()).toDF().write.mode(SaveMode.Overwrite).option("path", s"$outputDir/clinvar")
        .format("parquet")
        .saveAsTable("clinvar")

      When("Join variants")
      JoinVariants.join(Seq(studyId1, studyId2), releaseId, outputDir)

      Then("A new table for the release is created")
      val variantReleaseTable = spark.table("variant.variants_re_abcdef")

      And("this table should contain all merged data")
      val output = variantReleaseTable
        .as[JoinVariantOutput]
      val expectedOutput = Seq(
        JoinVariantOutput(
          ac = 12, an = 27, af = 0.44444444, homozygotes = 9, heterozygotes = 7,
          by_study = Map(
            studyId1 -> Freq(10, 5, 0.5, 2, 3),
            studyId3 -> Freq(7, 2, 0.5, 5, 1),
            studyId2 -> Freq(10, 5, 0.5, 2, 3)
          )),
        JoinVariantOutput(
          chromosome = "3", start = 3000, end = 3000, reference = "T", alternate = "G", ac = 5, an = 20, af = 0.25, name = "mutation_2", hgvsg = "chr3:g.2000T>G", homozygotes = 1, heterozygotes = 4,
          topmed = None, gnomad_genomes_2_1 = None, clinvar_id = None, clin_sig = None,
          by_study = Map(
            studyId1 -> Freq(20, 5, 0.25, 1, 4)
          )),
        JoinVariantOutput(
          chromosome = "3", start = 3000, end = 3000, "C", "A", name = "mutation_2", hgvsg = "chr3:g.2000T>G", ac = 10, an = 30, af = 0.33333333, homozygotes = 2, heterozygotes = 8,
          topmed = None, gnomad_genomes_2_1 = None, clinvar_id = None, clin_sig = None,
          by_study = Map(
            studyId2 -> Freq(30, 10, 0.33333333, 2, 8)
          )),
        existingVariant2.copy(release_id = releaseId)
      )

      output.collect() should contain theSameElementsAs expectedOutput

    }
  }

}