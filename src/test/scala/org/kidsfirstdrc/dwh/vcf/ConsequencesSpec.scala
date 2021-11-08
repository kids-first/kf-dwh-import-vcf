package org.kidsfirstdrc.dwh.vcf

import bio.ferlab.datalake.commons.config.{Configuration, StorageConf}
import bio.ferlab.datalake.commons.file.FileSystemType.LOCAL
import org.kidsfirstdrc.dwh.conf.Catalog.{HarmonizedData, Public}
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.kidsfirstdrc.dwh.testutils.external.EnsemblMappingOutput
import org.kidsfirstdrc.dwh.testutils.vcf.{ConsequenceInput, ConsequenceOutput, ConsequenceRowInput}
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ConsequencesSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {

  import spark.implicits._

  implicit val conf: Configuration =
    Configuration(
      List(StorageConf("kf-strides-variant", getClass.getClassLoader.getResource(".").getFile, LOCAL))
    )

  val studyId   = "SD_123456"
  val releaseId = "RE_ABCDEF"

  "build" should "return a dataframe with one consequecne by transcript" in {



    val df = Seq(
      ConsequenceRowInput(
        annotations = Seq(
          ConsequenceInput(),
          ConsequenceInput(
            Consequence = Seq("missense_variant", "NMD_transcript_variant"),
            Feature = "ENST00000636135.1"
          )
        )
      ),
      ConsequenceRowInput(
        annotations = Seq(
          ConsequenceInput()
        )
      )
    ).toDF()

    val output = new Consequences(studyId, releaseId, "", "")
      .transform(
        Map(HarmonizedData.family_variants_vcf.id -> df)
      )

    output.as[ConsequenceOutput].collect() should contain theSameElementsAs Seq(
      ConsequenceOutput(),
      ConsequenceOutput(
        ensembl_transcript_id = Some("ENST00000636135.1"),
        consequences = Seq("missense_variant", "NMD_transcript_variant")
      )
    )
  }

  it should "return a dataframe with one ConsequenceInput with regulatory feature" in {

    val df = Seq(
      ConsequenceRowInput(
        annotations = Seq(
          ConsequenceInput(
            CANONICAL = "NO",
            Feature_type = "Transcript",
            Feature = "ENST00000332831"
          ),
          ConsequenceInput(Feature_type = "RegulatoryFeature", Feature = "ENSR0000636135")
        )
      )
    ).toDF()

    val output =
      new Consequences(studyId, releaseId, "", "")
        .transform(
          Map(HarmonizedData.family_variants_vcf.id -> df)
        )

    output.as[ConsequenceOutput].collect() should contain theSameElementsAs Seq(
      ConsequenceOutput(
        ensembl_transcript_id = Some("ENST00000332831"),
        ensembl_regulatory_id = None,
        feature_type = "Transcript",
        original_canonical = false
      ),
      ConsequenceOutput(
        ensembl_transcript_id = None,
        ensembl_regulatory_id = Some("ENSR0000636135"),
        feature_type = "RegulatoryFeature"
      )
    )
  }

}
