package org.kidsfirstdrc.dwh.vcf

import org.kidsfirstdrc.dwh.testutils.Model.{ConsequenceInput, _}
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class ConsequencesSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {


  import spark.implicits._

  val studyId = "SD_123456"
  val releaseId = "RE_ABCDEF"

  "build" should "return a dataframe with one consequecne by transcript" in {
    val df = Seq(
      VCFRowInput(
        INFO_ANN = Seq(
          ConsequenceInput(),
          ConsequenceInput(Consequence  = Seq("missense_variant", "NMD_transcript_variant"), Feature = "ENST00000636135.1")
        )
      ),
      VCFRowInput(
        INFO_ANN = Seq(
          ConsequenceInput()
        )
      )
    ).toDF()

    val output = Consequences.build(studyId, releaseId, df)
    output.as[ConsequenceOutput].collect() should contain theSameElementsAs Seq(
      ConsequenceOutput(),
      ConsequenceOutput(ensembl_transcript_id = Some("ENST00000636135.1")),
      ConsequenceOutput(consequence = "NMD_transcript_variant", ensembl_transcript_id = Some("ENST00000636135.1"))
    )
  }

  it should "return a dataframe with one ConsequenceInput with regulatory feature" in {
    val df = Seq(
      VCFRowInput(
        INFO_ANN = Seq(
          ConsequenceInput(Feature_type = "RegulatoryFeature", Feature = "ENSR0000636135")
        )
      )
    ).toDF()

    val output = Consequences.build(studyId, releaseId, df)
    output.as[ConsequenceOutput].collect() should contain theSameElementsAs Seq(
      ConsequenceOutput(ensembl_transcript_id = None, ensembl_regulatory_id = Some("ENSR0000636135"), feature_type = "RegulatoryFeature")
    )
  }

}



