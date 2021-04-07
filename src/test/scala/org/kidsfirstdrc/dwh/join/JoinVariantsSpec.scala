package org.kidsfirstdrc.dwh.join

import org.apache.spark.sql.SaveMode
import org.kidsfirstdrc.dwh.testutils.Model._
import org.kidsfirstdrc.dwh.testutils.join.{Freq, JoinVariantOutput}
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.kidsfirstdrc.dwh.testutils.vcf.{VariantFrequency, VariantOutput}
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
      val variant1 = VariantOutput(frequencies = VariantFrequency(), study_id = studyId1, consent_codes = Set(s"$studyId1.c1"), consent_codes_by_study = Map(studyId1 -> Set(s"$studyId1.c1")))
      val variant2 = VariantOutput(chromosome = "3", start = 3000, end = 3000, reference = "T", alternate = "G", frequencies = VariantFrequency(),//ac = 5, an = 20, af = 0.25, homozygotes = 1, heterozygotes = 5,
        name = Some("mutation_2"), hgvsg = "chr3:g.2000T>G", study_id = studyId1, consent_codes = Set(s"$studyId1.c2"), consent_codes_by_study = Map(studyId1 -> Set(s"$studyId1.c2")))

      Seq(variant1, variant2).toDF().write.mode(SaveMode.Overwrite)
        .option("path", s"$outputDir/variants_sd_123_re_abcdef")
        .format("parquet")
        .saveAsTable("variants_sd_123_re_abcdef")

      //Study 2
      val variant3 = VariantOutput(chromosome = "3", start = 3000, end = 3000, "C", "A", name = Some("mutation_2"), hgvsg = "chr3:g.2000T>G",
        frequencies = VariantFrequency(), //ac = 10, an = 30, af = 0.33333333, homozygotes = 2, heterozygotes = 8,
        study_id = studyId2, consent_codes = Set(s"$studyId2.c0"), consent_codes_by_study = Map(studyId2 -> Set(s"$studyId2.c0")))
      val variant4 = variant1.copy(study_id = studyId2, consent_codes = Set(s"$studyId2.c1"), consent_codes_by_study = Map(studyId2 -> Set(s"$studyId2.c1")))

      Seq(variant3, variant4).toDF().write.mode(SaveMode.Overwrite)
        .option("path", s"$outputDir/variants_sd_456_re_abcdef")
        .format("parquet")
        .saveAsTable("variants_sd_456_re_abcdef")

      Given("1 existing table annotation that contains some data for at least one study")
      val studyId3 = "SD_789"
      val existingVariant1 = JoinVariantOutput(
        frequencies = VariantFrequency(Freq(ac = 4, an = 10, af = 0.4, homozygotes = 6, heterozygotes = 2), Freq(ac = 4, an = 10, af = 0.4, homozygotes = 6, heterozygotes = 2)),
        upper_bound_kf_ac_by_study = Map(studyId1 -> 2, studyId3 -> 2),
        upper_bound_kf_an_by_study = Map(studyId1 -> 3, studyId3 -> 7),
        upper_bound_kf_af_by_study = Map(studyId1 -> 0.66666667, studyId3 -> 0.5),
        upper_bound_kf_homozygotes_by_study = Map(studyId1 -> 1, studyId3 -> 5),
        upper_bound_kf_heterozygotes_by_study = Map(studyId1 -> 1, studyId3 -> 1),
        lower_bound_kf_ac_by_study = Map(studyId1 -> 2, studyId3 -> 2),
        lower_bound_kf_an_by_study = Map(studyId1 -> 3, studyId3 -> 7),
        lower_bound_kf_af_by_study = Map(studyId1 -> 0.66666667, studyId3 -> 0.5),
        lower_bound_kf_homozygotes_by_study = Map(studyId1 -> 1, studyId3 -> 5),
        lower_bound_kf_heterozygotes_by_study = Map(studyId1 -> 1, studyId3 -> 1),
        studies = Set(studyId1, studyId3), release_id = "RE_PREVIOUS",
        consent_codes = Set(s"$studyId1.c99", s"$studyId3.c99"),
        consent_codes_by_study = Map(studyId1 -> Set(s"$studyId1.c99"), studyId3 -> Set(s"$studyId3.c99")))

      val removedOldVariant = JoinVariantOutput(alternate = "G",
        upper_bound_kf_ac_by_study = Map(studyId1 -> 75),
        upper_bound_kf_an_by_study = Map(studyId1 -> 100),
        upper_bound_kf_af_by_study = Map(studyId1 -> 0.75),
        upper_bound_kf_homozygotes_by_study = Map(studyId1 -> 30),
        upper_bound_kf_heterozygotes_by_study = Map(studyId1 -> 20),
        lower_bound_kf_ac_by_study = Map(studyId1 -> 75),
        lower_bound_kf_an_by_study = Map(studyId1 -> 100),
        lower_bound_kf_af_by_study = Map(studyId1 -> 0.75),
        lower_bound_kf_homozygotes_by_study = Map(studyId1 -> 30),
        lower_bound_kf_heterozygotes_by_study = Map(studyId1 -> 20),
        studies = Set(studyId1),
        release_id = "RE_PREVIOUS",
        consent_codes = Set(s"$studyId1.c99"),
        consent_codes_by_study = Map(studyId1 -> Set(s"$studyId1.c99")))

      val existingVariant2 = JoinVariantOutput(
        chromosome = "4", start = 4000, end = 4000, reference = "T", alternate = "G",
        frequencies = VariantFrequency(Freq(ac = 2, an = 3, af = 0.6666666667, homozygotes = 1, heterozygotes = 1), Freq(ac = 2, an = 3, af = 0.6666666667, homozygotes = 1, heterozygotes = 1)),
        upper_bound_kf_ac_by_study = Map(studyId3 -> 2),
        upper_bound_kf_an_by_study = Map(studyId3 -> 3),
        upper_bound_kf_af_by_study = Map(studyId3 -> 0.6666666667),
        upper_bound_kf_homozygotes_by_study = Map(studyId3 -> 1),
        upper_bound_kf_heterozygotes_by_study = Map(studyId3 -> 1),
        lower_bound_kf_ac_by_study = Map(studyId3 -> 2),
        lower_bound_kf_an_by_study = Map(studyId3 -> 3),
        lower_bound_kf_af_by_study = Map(studyId3 -> 0.6666666667),
        lower_bound_kf_homozygotes_by_study = Map(studyId3 -> 1),
        lower_bound_kf_heterozygotes_by_study = Map(studyId3 -> 1),
        studies = Set(studyId3),
        topmed = None, gnomad_genomes_2_1 = None, clinvar_id = None, clin_sig = None, dbsnp_id = None,
        release_id = "RE_PREVIOUS",
        consent_codes = Set(s"$studyId3.c99"),
        consent_codes_by_study = Map(studyId3 -> Set(s"$studyId3.c99")))

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
      Seq(GnomadFrequencyEntry()).toDF().write.mode(SaveMode.Overwrite).option("path", s"$outputDir/gnomad_genomes_2_1_1_liftover_grch38")
        .format("parquet")
        .saveAsTable("gnomad_genomes_2_1_1_liftover_grch38")

      And("A table gnomad_exomes_2_1_1_liftover_grch38 exists")
      Seq(GnomadFrequencyEntry()).toDF().write.mode(SaveMode.Overwrite).option("path", s"$outputDir/gnomad_exomes_2_1_1_liftover_grch38")
        .format("parquet")
        .saveAsTable("gnomad_exomes_2_1_1_liftover_grch38")

      And("A table gnomad_genomes_3_0 exists")
      Seq(GnomadFrequencyEntry()).toDF().write.mode(SaveMode.Overwrite).option("path", s"$outputDir/gnomad_genomes_3_0")
        .format("parquet")
        .saveAsTable("gnomad_genomes_3_0")

      And("A table clinvar exists")
      Seq(ClinvarEntry()).toDF().write.mode(SaveMode.Overwrite).option("path", s"$outputDir/clinvar")
        .format("parquet")
        .saveAsTable("clinvar")

      And("A table dbsnp exists")
      Seq(DBSNPEntry()).toDF().write.mode(SaveMode.Overwrite).option("path", s"$outputDir/dbsnp")
        .format("parquet")
        .saveAsTable("dbsnp")

      When("Join variants")
      JoinVariants.join(Seq(studyId1, studyId2), releaseId, outputDir, mergeWithExisting = true, "variant")

      Then("A new table for the release is created")
      val variantReleaseTable = spark.table("variant.variants_re_abcdef")

      variantReleaseTable.show(false)

      And("this table should contain all merged data")
      val output = variantReleaseTable
        .withColumnRenamed("1k_genomes", "one_thousand_genomes")
        .as[JoinVariantOutput]

      val expectedOutput = Seq(
        JoinVariantOutput(
          frequencies = VariantFrequency(Freq(11,6,0.5454545455,7,1),Freq(11,6,0.5454545455,7,1)),
          upper_bound_kf_ac_by_study = Map(studyId3 -> 2, studyId1 -> 2, studyId2 -> 2),
          upper_bound_kf_an_by_study = Map(studyId3 -> 7, studyId1 -> 2, studyId2 -> 2),
          upper_bound_kf_af_by_study = Map(studyId3 -> 0.2857142857, studyId1 -> 1, studyId2 -> 1),
          upper_bound_kf_homozygotes_by_study = Map(studyId3 -> 5, studyId1 -> 1, studyId2 -> 1),
          upper_bound_kf_heterozygotes_by_study = Map(studyId3 -> 1, studyId1 -> 0, studyId2 -> 0),
          lower_bound_kf_ac_by_study = Map(studyId3 -> 2, studyId1 -> 2, studyId2 -> 2),
          lower_bound_kf_an_by_study = Map(studyId3 -> 7, studyId1 -> 2, studyId2 -> 2),
          lower_bound_kf_af_by_study = Map(studyId3 -> 0.2857142857, studyId1 -> 1, studyId2 -> 1),
          lower_bound_kf_homozygotes_by_study = Map(studyId3 -> 5, studyId1 -> 1, studyId2 -> 1),
          lower_bound_kf_heterozygotes_by_study = Map(studyId3 -> 1, studyId1 -> 0, studyId2 -> 0),
          studies = Set(studyId1, studyId2, studyId3),
          consent_codes = Set("SD_789.c99", "SD_123.c1", "SD_456.c1"),
          consent_codes_by_study = Map(studyId1 -> Set("SD_123.c1"), studyId2 -> Set("SD_456.c1"), studyId3 -> Set(s"$studyId3.c99"))),
        JoinVariantOutput(
          chromosome = "3", start = 3000, end = 3000, reference = "T", alternate = "G",
          frequencies = VariantFrequency(Freq(2,2,1,1,0),Freq(2,2,1,1,0)),
          upper_bound_kf_ac_by_study = Map(studyId1 -> 2),
          upper_bound_kf_an_by_study = Map(studyId1 -> 2),
          upper_bound_kf_af_by_study = Map(studyId1 -> 1.0000000000),
          upper_bound_kf_homozygotes_by_study = Map(studyId1 -> 1),
          upper_bound_kf_heterozygotes_by_study = Map(studyId1 -> 0),
          lower_bound_kf_ac_by_study = Map(studyId1 -> 2),
          lower_bound_kf_an_by_study = Map(studyId1 -> 2),
          lower_bound_kf_af_by_study = Map(studyId1 -> 1.0000000000),
          lower_bound_kf_homozygotes_by_study = Map(studyId1 -> 1),
          lower_bound_kf_heterozygotes_by_study = Map(studyId1 -> 0),
          name = "mutation_2", hgvsg = "chr3:g.2000T>G",
          topmed = None, gnomad_genomes_2_1 = None, clinvar_id = None, clin_sig = None, dbsnp_id = None,
          studies = Set(studyId1),
          consent_codes = variant2.consent_codes,
          consent_codes_by_study = Map(studyId1 -> variant2.consent_codes),
          one_thousand_genomes = None,
          gnomad_exomes_2_1 = None,
          gnomad_genomes_3_0 = None),
        JoinVariantOutput(
          chromosome = "3", start = 3000, end = 3000, "C", "A", name = "mutation_2", hgvsg = "chr3:g.2000T>G",
          frequencies = VariantFrequency(Freq(2,2,1,1,0),Freq(2,2,1,1,0)),
          upper_bound_kf_ac_by_study = Map(studyId2 -> 2),
          upper_bound_kf_an_by_study = Map(studyId2 -> 2),
          upper_bound_kf_af_by_study = Map(studyId2 -> 1.0000000000),
          upper_bound_kf_homozygotes_by_study = Map(studyId2 -> 1),
          upper_bound_kf_heterozygotes_by_study = Map(studyId2 -> 0),
          lower_bound_kf_ac_by_study = Map(studyId2 -> 2),
          lower_bound_kf_an_by_study = Map(studyId2 -> 2),
          lower_bound_kf_af_by_study = Map(studyId2 -> 1.0000000000),
          lower_bound_kf_homozygotes_by_study = Map(studyId2 -> 1),
          lower_bound_kf_heterozygotes_by_study = Map(studyId2 -> 0),
          topmed = None, gnomad_genomes_2_1 = None, clinvar_id = None, clin_sig = None, dbsnp_id = None,
          studies = Set(studyId2),
          consent_codes = variant3.consent_codes,
          consent_codes_by_study = Map(studyId2 -> variant3.consent_codes),
          one_thousand_genomes = None,
          gnomad_exomes_2_1 = None,
          gnomad_genomes_3_0 = None),
        existingVariant2.copy(release_id = releaseId,
          one_thousand_genomes = None,
          gnomad_exomes_2_1 = None,
          gnomad_genomes_3_0 = None)
      )
      output.collect() should contain theSameElementsAs expectedOutput

    }
  }

}
