package org.kidsfirstdrc.dwh.vcf

import bio.ferlab.datalake.commons.config.{Configuration, StorageConf}
import org.kidsfirstdrc.dwh.conf.Catalog.{Clinical, Raw}
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.kidsfirstdrc.dwh.testutils.join.Freq
import org.kidsfirstdrc.dwh.testutils.vcf.{OccurrenceOutput, ParticipantSaveSet, VariantFrequency, VariantOutput}
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class VariantsSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {

  import spark.implicits._

  spark.sql(s"CREATE DATABASE IF NOT EXISTS variant")
  spark.sql(s"CREATE DATABASE IF NOT EXISTS portal")

  val studyId   = "SD_123456"
  val releaseId = "RE_ABCDEF"

  "transform 'portal'" should "should keep only PT_001 and PT_002" in {

    implicit val conf: Configuration = {
      Configuration(
        List(
          StorageConf(
            "kf-strides-variant",
            getClass.getClassLoader.getResource(".").getFile + "portal"
          )
        )
      )
    }

    val occurrencesDf = Seq(
      OccurrenceOutput(
        `participant_id` = "PT_001",
        `zygosity` = "HET",
        `hgvsg` = null,
        `transmission` = Some("autosomal_recessive")),
      OccurrenceOutput(
        `participant_id` = "PT_002",
        `zygosity` = null,
        `hgvsg` = "chr4:g.73979437G>T",
        `transmission` = Some("autosomal_dominant")
      ),
      //should be dropped because PT_003 missing from participants table
      OccurrenceOutput(
        `participant_id` = "PT_003",
        `zygosity` = "HOM",
        `hgvsg` = "chr4:g.73979437G>T",
        `transmission` = Some("autosomal_dominant")
      )
    ).toDF()

    val participantsDf = Seq(
      ParticipantSaveSet(`id` = "PT_001"),
      ParticipantSaveSet(`id` = "PT_002")
    ).toDF()

    val data = Map(
      Clinical.occurrences.id -> occurrencesDf,
      Raw.all_participants.id -> participantsDf
    )

    val output = new Variants(studyId, releaseId, "portal").transform(data)

    output.as[VariantOutput].collect() should contain theSameElementsAs Seq(
      VariantOutput(
        "2",
        165310407,
        165310407,
        "G",
        "A",
        "chr4:g.73979437G>T",
        None,
        VariantFrequency(Freq(4, 1, 0.25, 0, 1), Freq(2, 1, 0.5, 0, 1)),
        "SNV",
        "SD_123456",
        "RE_ABCDEF",
        Set("phs001738.c1"),
        Map("SD_123456" -> Set("phs001738.c1")),
        transmissions = Map("autosomal_dominant" -> 1, "autosomal_recessive" -> 1),
        transmissions_by_study = Map("SD_123456" -> Map("autosomal_dominant" -> 1, "autosomal_recessive" -> 1)),
        List("HET")
      )
    )

    new Variants(studyId, releaseId, "portal").load(output)

    spark.table(s"portal.variants_${studyId.toLowerCase}_${releaseId.toLowerCase}").show(false)
  }

  "transform 'variant'" should "return a dataframe with aggregated frequencies by duo code" in {

    implicit val conf: Configuration =
      Configuration(
        List(StorageConf("kf-strides-variant", getClass.getClassLoader.getResource(".").getFile))
      )

    val occurrencesDf = Seq(
      OccurrenceOutput(
        `participant_id` = "PT_000001",
        `zygosity` = "HOM",
        `has_alt` = 1,
        `dbgap_consent_code` = "SD_123456.c1",
        `transmission` = Some("autosomal_dominant")
      ),
      OccurrenceOutput(
        `participant_id` = "PT_000002",
        `zygosity` = "HET",
        `has_alt` = 1,
        `dbgap_consent_code` = "SD_123456.c2",
        `transmission` = Some("autosomal_dominant")
      ),
      OccurrenceOutput(
        `participant_id` = "PT_000003",
        `zygosity` = "UNK",
        `has_alt` = 0,
        `dbgap_consent_code` = "SD_123456.c3"
      )
    ).toDF()

    val participantsDf = Seq(ParticipantSaveSet()).toDF()

    val data = Map(
      Clinical.occurrences.id -> occurrencesDf,
      Raw.all_participants.id -> participantsDf
    )

    val output = new Variants(studyId, releaseId, "variant").transform(data)

    output.as[VariantOutput].collect() should contain theSameElementsAs Seq(
      VariantOutput(
        "2",
        165310407,
        165310407,
        "G",
        "A",
        "chr4:g.73979437G>T",
        None,
        frequencies = VariantFrequency(Freq(6, 3, 0.5, 1, 1), Freq(4, 3, 0.75, 1, 1)),
        consent_codes = Set("SD_123456.c1", "SD_123456.c2", "SD_123456.c3"),
        consent_codes_by_study =
          Map("SD_123456" -> Set("SD_123456.c1", "SD_123456.c2", "SD_123456.c3")),
        transmissions = Map("autosomal_dominant" -> 2),
        transmissions_by_study = Map("SD_123456" -> Map("autosomal_dominant" -> 2)),
        zygosity = List("UNK", "HET", "HOM")
      )
    )

    spark.sql("use variant")
    new Variants(studyId, releaseId, "variant").load(output)
    spark.table(s"variants_${studyId.toLowerCase}_${releaseId.toLowerCase}").show(false)
    output.write
      .mode("overwrite")
      .json(this.getClass.getClassLoader.getResource(".").getFile + "variants")
  }

}
