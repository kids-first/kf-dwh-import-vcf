package org.kidsfirstdrc.dwh.join

import bio.ferlab.datalake.commons.config.{Configuration, StorageConf}
import org.apache.spark.sql.SaveMode
import org.kidsfirstdrc.dwh.conf.Catalog.{Clinical, Public}
import org.kidsfirstdrc.dwh.testutils.Model._
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.kidsfirstdrc.dwh.testutils.external.GnomadV311Output
import org.kidsfirstdrc.dwh.testutils.join.{Freq, JoinVariantOutput}
import org.kidsfirstdrc.dwh.testutils.vcf.{VariantFrequency, VariantOutput}
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class JoinVariantsSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {

  import spark.implicits._

  val releaseId = "RE_ABCDEF"

  "build" should "return a dataframe with all expected columns" in {
    withOutputFolder("output") { outputDir =>
      implicit val conf: Configuration = Configuration(
        List(
          StorageConf("kf-strides-variant", outputDir)
        )
      )

      spark.sql("create database if not exists variant")
      spark.sql("use variant")
      Given("2 studies")
      val (studyId1, studyId2) = ("SD_123", "SD_456")

      Given("2 tables, one  for each study")
      //Study 1
      val variant1 = VariantOutput(
        frequencies = VariantFrequency(),
        study_id = studyId1,
        consent_codes = Set(s"$studyId1.c1"),
        consent_codes_by_study = Map(studyId1 -> Set(s"$studyId1.c1")),
        transmissions = Map("AD" -> 3, "AR" -> 1),
        transmissions_by_study = Map(studyId1 -> Map("AD" -> 3, "AR" -> 1)),
        zygosity = List("HOM")
      )
      val variant2 = VariantOutput(
        chromosome = "3",
        start = 3000,
        end = 3000,
        reference = "T",
        alternate = "G",
        frequencies =
          VariantFrequency(), //ac = 5, an = 20, af = 0.25, homozygotes = 1, heterozygotes = 5,
        name = Some("mutation_2"),
        hgvsg = "chr3:g.2000T>G",
        study_id = studyId1,
        consent_codes = Set(s"$studyId1.c2"),
        consent_codes_by_study = Map(studyId1 -> Set(s"$studyId1.c2")),
        transmissions = Map("AD" -> 1),
        transmissions_by_study = Map(studyId1 -> Map("AD" -> 1)),
        zygosity = List("HET")
      )

      Seq(variant1, variant2)
        .toDF()
        .write
        .mode(SaveMode.Overwrite)
        .option("path", s"$outputDir/variants_sd_123_re_abcdef")
        .format("parquet")
        .saveAsTable("variants_sd_123_re_abcdef")

      //Study 2
      val variant3 = VariantOutput(
        chromosome = "3",
        start = 3000,
        end = 3000,
        "C",
        "A",
        name = Some("mutation_2"),
        hgvsg = "chr3:g.2000T>G",
        frequencies =
          VariantFrequency(), //ac = 10, an = 30, af = 0.33333333, homozygotes = 2, heterozygotes = 8,
        study_id = studyId2,
        consent_codes = Set(s"$studyId2.c0"),
        consent_codes_by_study = Map(studyId2 -> Set(s"$studyId2.c0")),
        transmissions = Map("AD" -> 1),
        transmissions_by_study = Map(studyId2 -> Map("AD" -> 1)),
        zygosity = List("WT")
      )
      val variant4 = variant1.copy(
        study_id = studyId2,
        consent_codes = Set(s"$studyId2.c1"),
        consent_codes_by_study = Map(studyId2 -> Set(s"$studyId2.c1")),
        transmissions = Map("AD" -> 1),
        transmissions_by_study = Map(studyId2 -> Map("AD" -> 1))
      )

      Seq(variant3, variant4)
        .toDF()
        .write
        .mode(SaveMode.Overwrite)
        .option("path", s"$outputDir/variants_sd_456_re_abcdef")
        .format("parquet")
        .saveAsTable("variants_sd_456_re_abcdef")

      Given("1 existing table annotation that contains some data for at least one study")
      val studyId3 = "SD_789"
      val existingVariant1 = JoinVariantOutput(
        frequencies = VariantFrequency(
          Freq(ac = 4, an = 10, af = 0.4, homozygotes = 6, heterozygotes = 2),
          Freq(ac = 4, an = 10, af = 0.4, homozygotes = 6, heterozygotes = 2)
        ),
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
        studies = Set(studyId1, studyId3),
        release_id = "RE_PREVIOUS",
        consent_codes = Set(s"$studyId1.c99", s"$studyId3.c99"),
        consent_codes_by_study = Map(studyId1 -> Set(s"$studyId1.c99"), studyId3 -> Set(s"$studyId3.c99")),
        transmissions = Map("AD" -> 1),
        transmissions_by_study = Map(studyId3 -> Map("AD" -> 1))
      )

      val removedOldVariant = JoinVariantOutput(
        alternate = "G",
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
        consent_codes_by_study = Map(studyId1 -> Set(s"$studyId1.c99")),
        transmissions = Map("AD" -> 1),
        transmissions_by_study = Map(studyId1 -> Map("AD" -> 1))
      )

      val existingVariant2 = JoinVariantOutput(
        chromosome = "4",
        start = 4000,
        end = 4000,
        reference = "T",
        alternate = "G",
        frequencies = VariantFrequency(
          Freq(ac = 2, an = 3, af = 0.6666666666666666, homozygotes = 1, heterozygotes = 1),
          Freq(ac = 2, an = 3, af = 0.6666666666666666, homozygotes = 1, heterozygotes = 1)
        ),
        upper_bound_kf_ac_by_study = Map(studyId3 -> 2),
        upper_bound_kf_an_by_study = Map(studyId3 -> 3),
        upper_bound_kf_af_by_study = Map(studyId3 -> 0.6666666666666666),
        upper_bound_kf_homozygotes_by_study = Map(studyId3 -> 1),
        upper_bound_kf_heterozygotes_by_study = Map(studyId3 -> 1),
        lower_bound_kf_ac_by_study = Map(studyId3 -> 2),
        lower_bound_kf_an_by_study = Map(studyId3 -> 3),
        lower_bound_kf_af_by_study = Map(studyId3 -> 0.6666666666666666),
        lower_bound_kf_homozygotes_by_study = Map(studyId3 -> 1),
        lower_bound_kf_heterozygotes_by_study = Map(studyId3 -> 1),
        studies = Set(studyId3),
        topmed = None,
        gnomad_genomes_2_1 = None,
        clinvar_id = None,
        clin_sig = None,
        dbsnp_id = None,
        release_id = "RE_PREVIOUS",
        consent_codes = Set(s"$studyId3.c99"),
        consent_codes_by_study = Map(studyId3 -> Set(s"$studyId3.c99")),
        transmissions = Map("AD" -> 1),
        transmissions_by_study = Map(studyId3 -> Map("AD" -> 1))
      )

      Seq(existingVariant1, removedOldVariant, existingVariant2)
        .toDF()
        .write
        .mode(SaveMode.Overwrite)
        .option("path", s"$outputDir/variants")
        .format("parquet")
        .saveAsTable("variants")

      And("A table 1000_genomes exists")
      Seq(FrequencyEntry())
        .toDF()
        .write
        .mode(SaveMode.Overwrite)
        .option("path", s"$outputDir/1000_genomes")
        .format("parquet")
        .saveAsTable("1000_genomes")

      And("A table topmed_bravo exists")
      Seq(FrequencyEntry())
        .toDF()
        .write
        .mode(SaveMode.Overwrite)
        .option("path", s"$outputDir/topmed_bravo")
        .format("parquet")
        .saveAsTable("topmed_bravo")

      And("A table gnomad_genomes_2_1_1_liftover_grch38 exists")
      Seq(GnomadFrequencyEntry())
        .toDF()
        .write
        .mode(SaveMode.Overwrite)
        .option("path", s"$outputDir/gnomad_genomes_2_1_1_liftover_grch38")
        .format("parquet")
        .saveAsTable("gnomad_genomes_2_1_1_liftover_grch38")

      And("A table gnomad_exomes_2_1_1_liftover_grch38 exists")
      Seq(GnomadFrequencyEntry())
        .toDF()
        .write
        .mode(SaveMode.Overwrite)
        .option("path", s"$outputDir/gnomad_exomes_2_1_1_liftover_grch38")
        .format("parquet")
        .saveAsTable("gnomad_exomes_2_1_1_liftover_grch38")

      And("A table gnomad_genomes_3_0 exists")
      Seq(GnomadFrequencyEntry())
        .toDF()
        .write
        .mode(SaveMode.Overwrite)
        .option("path", s"$outputDir/gnomad_genomes_3_0")
        .format("parquet")
        .saveAsTable("gnomad_genomes_3_0")

      And("A table gnomad_genomes_3_1_1 exists")
      Seq(GnomadV311Output())
        .toDF()
        .write
        .mode(SaveMode.Overwrite)
        .option("path", s"$outputDir/gnomad_genomes_3_1_1")
        .format("parquet")
        .saveAsTable("gnomad_genomes_3_1_1")

      And("A table clinvar exists")
      Seq(ClinvarEntry())
        .toDF()
        .write
        .mode(SaveMode.Overwrite)
        .option("path", s"$outputDir/clinvar")
        .format("parquet")
        .saveAsTable("clinvar")

      And("A table dbsnp exists")
      Seq(DBSNPEntry())
        .toDF()
        .write
        .mode(SaveMode.Overwrite)
        .option("path", s"$outputDir/dbsnp")
        .format("parquet")
        .saveAsTable("dbsnp")

      When("Join variants")
      new JoinVariants(Seq(studyId1, studyId2), releaseId, mergeWithExisting = true, "variant")
        .run()

      Then("A new table for the release is created")
      val variantReleaseTable = spark.table("variant.variants_re_abcdef")

      variantReleaseTable.show(false)

      And("this table should contain all merged data")
      val output = variantReleaseTable
        .withColumnRenamed("1k_genomes", "one_thousand_genomes")
        .as[JoinVariantOutput]

      val expectedOutput = Seq(
        JoinVariantOutput(
          frequencies = VariantFrequency(
            Freq(11, 6, 0.5454545454545454, 7, 1),
            Freq(11, 6, 0.5454545454545454, 7, 1)
          ),
          upper_bound_kf_ac_by_study = Map(studyId3 -> 2, studyId1 -> 2, studyId2 -> 2),
          upper_bound_kf_an_by_study = Map(studyId3 -> 7, studyId1 -> 2, studyId2 -> 2),
          upper_bound_kf_af_by_study =
            Map(studyId3 -> 0.2857142857142857, studyId1 -> 1, studyId2 -> 1),
          upper_bound_kf_homozygotes_by_study = Map(studyId3 -> 5, studyId1 -> 1, studyId2 -> 1),
          upper_bound_kf_heterozygotes_by_study = Map(studyId3 -> 1, studyId1 -> 0, studyId2 -> 0),
          lower_bound_kf_ac_by_study = Map(studyId3 -> 2, studyId1 -> 2, studyId2 -> 2),
          lower_bound_kf_an_by_study = Map(studyId3 -> 7, studyId1 -> 2, studyId2 -> 2),
          lower_bound_kf_af_by_study =
            Map(studyId3 -> 0.2857142857142857, studyId1 -> 1, studyId2 -> 1),
          lower_bound_kf_homozygotes_by_study = Map(studyId3 -> 5, studyId1 -> 1, studyId2 -> 1),
          lower_bound_kf_heterozygotes_by_study = Map(studyId3 -> 1, studyId1 -> 0, studyId2 -> 0),
          studies = Set(studyId1, studyId2, studyId3),
          consent_codes = Set("SD_789.c99", "SD_123.c1", "SD_456.c1"),
          consent_codes_by_study = Map(
            studyId1 -> Set("SD_123.c1"),
            studyId2 -> Set("SD_456.c1"),
            studyId3 -> Set(s"$studyId3.c99")
          ),
          transmissions = Map("AD" -> 5, "AR" -> 1),
          transmissions_by_study = Map(studyId1 -> Map("AD" -> 3, "AR" -> 1), studyId2 -> Map("AD" -> 1), studyId3 -> Map("AD" -> 1)),
        ),
        JoinVariantOutput(
          chromosome = "3",
          start = 3000,
          end = 3000,
          reference = "T",
          alternate = "G",
          frequencies =
            VariantFrequency(Freq(11, 2, 0.18181818181818182, 1, 0), Freq(2, 2, 1.0, 1, 0)),
          upper_bound_kf_ac_by_study = Map(studyId1 -> 2),
          upper_bound_kf_an_by_study = Map(studyId1 -> 2),
          upper_bound_kf_af_by_study = Map(studyId1 -> 1),
          upper_bound_kf_homozygotes_by_study = Map(studyId1 -> 1),
          upper_bound_kf_heterozygotes_by_study = Map(studyId1 -> 0),
          lower_bound_kf_ac_by_study = Map(studyId1 -> 2),
          lower_bound_kf_an_by_study = Map(studyId1 -> 2),
          lower_bound_kf_af_by_study = Map(studyId1 -> 1),
          lower_bound_kf_homozygotes_by_study = Map(studyId1 -> 1),
          lower_bound_kf_heterozygotes_by_study = Map(studyId1 -> 0),
          name = "mutation_2",
          hgvsg = "chr3:g.2000T>G",
          topmed = None,
          gnomad_genomes_2_1 = None,
          clinvar_id = None,
          clin_sig = None,
          dbsnp_id = None,
          studies = Set(studyId1),
          consent_codes = variant2.consent_codes,
          consent_codes_by_study = Map(studyId1 -> variant2.consent_codes),
          transmissions = Map("AD" -> 1),
          transmissions_by_study = Map(studyId1 -> Map("AD" -> 1)),
          one_thousand_genomes = None,
          gnomad_exomes_2_1 = None,
          gnomad_genomes_3_0 = None,
          gnomad_genomes_3_1_1 = None,
          zygosity = List("HET")
        ),
        JoinVariantOutput(
          chromosome = "3",
          start = 3000,
          end = 3000,
          "C",
          "A",
          name = "mutation_2",
          hgvsg = "chr3:g.2000T>G",
          frequencies =
            VariantFrequency(Freq(11, 2, 0.18181818181818182, 1, 0), Freq(2, 2, 1.0, 1, 0)),
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
          topmed = None,
          gnomad_genomes_2_1 = None,
          clinvar_id = None,
          clin_sig = None,
          dbsnp_id = None,
          studies = Set(studyId2),
          consent_codes = variant3.consent_codes,
          consent_codes_by_study = Map(studyId2 -> variant3.consent_codes),
          transmissions = Map("AD" -> 1),
          transmissions_by_study = Map(studyId2 -> Map("AD" -> 1)),
          one_thousand_genomes = None,
          gnomad_exomes_2_1 = None,
          gnomad_genomes_3_0 = None,
          gnomad_genomes_3_1_1 = None,
          zygosity = List("WT")
        ),
        existingVariant2.copy(
          release_id = releaseId,
          one_thousand_genomes = None,
          gnomad_exomes_2_1 = None,
          gnomad_genomes_3_0 = None,
          gnomad_genomes_3_1_1 = None,
          frequencies = VariantFrequency(
            Freq(11, 2, 0.18181818181818182, 1, 1),
            Freq(3, 2, 0.6666666666666666, 1, 1)
          )
        )
      )
      output.collect() should contain theSameElementsAs expectedOutput
    }
  }

  "join variant" should "combine frequencies" in {
    withOutputFolder("output") { outputDir =>
      implicit val conf: Configuration = Configuration(
        List(
          StorageConf("kf-strides-variant", getClass.getResource(".").getFile)
        )
      )

      val studyId3 = "SD_333"

      spark.sql("create database if not exists variant")
      spark.sql("drop table if exists variant.variants")
      spark.sql("use variant")

      val existingVariant1 =
        Seq(
          JoinVariantOutput(
            "2",
            165310407,
            165310407,
            "G",
            "A",
            frequencies = VariantFrequency(
              Freq(ac = 4, an = 10, af = 0.4, homozygotes = 6, heterozygotes = 2),
              Freq(ac = 4, an = 10, af = 0.4, homozygotes = 6, heterozygotes = 2)
            ),
            upper_bound_kf_ac_by_study = Map(studyId3 -> 4),
            upper_bound_kf_an_by_study = Map(studyId3 -> 10),
            upper_bound_kf_af_by_study = Map(studyId3 -> 0.4),
            upper_bound_kf_homozygotes_by_study = Map(studyId3 -> 6),
            upper_bound_kf_heterozygotes_by_study = Map(studyId3 -> 2),
            lower_bound_kf_ac_by_study = Map(studyId3 -> 4),
            lower_bound_kf_an_by_study = Map(studyId3 -> 10),
            lower_bound_kf_af_by_study = Map(studyId3 -> 0.4),
            lower_bound_kf_homozygotes_by_study = Map(studyId3 -> 6),
            lower_bound_kf_heterozygotes_by_study = Map(studyId3 -> 2),
            studies = Set(studyId3),
            release_id = "RE_PREVIOUS",
            consent_codes = Set(s"$studyId3.c99"),
            consent_codes_by_study = Map(studyId3 -> Set(s"$studyId3.c99")),
            transmissions = Map("AD" -> 1),
            transmissions_by_study = Map(studyId3 -> Map("AD" -> 1)),
          )
        ).toDF.write
          .mode(SaveMode.Overwrite)
          .format("parquet")
          .option("path", outputDir + "variants")
          .saveAsTable("variant.variants")

      val variants = Seq(
        VariantOutput(
          "2",
          165310407,
          165310407,
          "G",
          "A",
          "chr4:g.73979437G>A",
          None,
          frequencies = VariantFrequency(Freq(6, 3, 0.5, 1, 1), Freq(4, 3, 0.75, 1, 1)),
          consent_codes = Set("SD_111.c1"),
          study_id = "SD_111",
          consent_codes_by_study = Map("SD_111" -> Set("SD_111.c1")),
          transmissions = Map("AD" -> 1),
          transmissions_by_study = Map("SD_111" -> Map("AD" -> 1)),
          zygosity = List("HOM")
        ),
        VariantOutput(
          "2",
          165310407,
          165310407,
          "G",
          "C",
          "chr4:g.73979437G>C",
          None,
          frequencies = VariantFrequency(Freq(6, 3, 0.5, 1, 1), Freq(4, 3, 0.75, 1, 1)),
          consent_codes = Set("SD_222.c1"),
          study_id = "SD_222",
          consent_codes_by_study = Map("SD_222" -> Set("SD_222.c1")),
          transmissions = Map("AD" -> 1),
          transmissions_by_study = Map("SD_222" -> Map("AD" -> 1)),
          zygosity = List("HET")
        )
      ).toDF()

      val data = Map(
        Public.`1000_genomes`.id       -> Seq(FrequencyEntry()).toDF(),
        Public.topmed_bravo.id         -> Seq(FrequencyEntry()).toDF(),
        Public.gnomad_genomes_2_1.id   -> Seq(GnomadFrequencyEntry()).toDF(),
        Public.gnomad_exomes_2_1.id    -> Seq(GnomadFrequencyEntry()).toDF(),
        Public.gnomad_genomes_3_0.id   -> Seq(GnomadFrequencyEntry()).toDF(),
        Public.gnomad_genomes_3_1_1.id -> Seq(GnomadV311Output()).toDF(),
        Public.clinvar.id              -> Seq(ClinvarEntry()).toDF(),
        Public.dbsnp.id                -> Seq(DBSNPEntry()).toDF(),
        Clinical.variants.id           -> variants
      )

      val result = new JoinVariants(Seq("SD_222", "SD_111"), "RE_000000", true, "variant")
        .transform(data)

      result.columns should contain allElementsOf List("gnomad_genomes_2_1", "gnomad_exomes_2_1", "gnomad_genomes_3_0", "gnomad_genomes_3_1_1")

      result
        .select(
          "frequencies.upper_bound_kf",
          "upper_bound_kf_ac_by_study",
          "upper_bound_kf_an_by_study",
          "upper_bound_kf_af_by_study"
        )
        .show(false)

      result
        .select("frequencies.upper_bound_kf.*")
        .as[Freq]
        .collect() should contain theSameElementsAs Seq(
        Freq(22, 7, 0.3181818181818182, 7, 3),
        Freq(22, 3, 0.13636363636363635, 1, 1)
      )

      result
        .select("upper_bound_kf_ac_by_study")
        .as[Map[String, Long]]
        .collect() should contain theSameElementsAs Seq(
        Map("SD_333" -> 4, "SD_111" -> 3),
        Map("SD_222" -> 3)
      )

      result
        .select("upper_bound_kf_an_by_study")
        .as[Map[String, Long]]
        .collect() should contain theSameElementsAs Seq(
        Map("SD_333" -> 10, "SD_111" -> 6),
        Map("SD_222" -> 6)
      )

      result
        .select("upper_bound_kf_af_by_study")
        .as[Map[String, Long]]
        .collect() should contain theSameElementsAs Seq(
        Map("SD_333" -> 0.4, "SD_111" -> 0.5),
        Map("SD_222" -> 0.5)
      )

    }
  }

}
