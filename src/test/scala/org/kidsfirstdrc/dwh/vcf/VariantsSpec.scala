package org.kidsfirstdrc.dwh.vcf

import org.kidsfirstdrc.dwh.testutils.Model._
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class VariantsSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {

  import spark.implicits._

  val studyId = "SD_123456"
  val releaseId = "RE_ABCDEF"
  "build" should "return a dataframe with all expected columns" in {
    val df = Seq(
      VariantInput()
    ).toDF()

    val output = Variants.build(studyId, releaseId, df)

    output.as[VariantOutput].collect() should contain theSameElementsAs Seq(
      VariantOutput()
    )
  }

  it should "return a dataframe with aggregated frequencies by duo code" in {
    val df = Seq(
      VariantInput(is_hmb = true, zygosity = "HOM", has_alt = 1),
      VariantInput(is_hmb = true, is_gru = true, zygosity = "HET", has_alt = 1),
      VariantInput(is_hmb = false, is_gru = true, zygosity = "HET", has_alt = 1)
    ).toDF()

    val output = Variants.build(studyId, releaseId, df)

    output.as[VariantOutput].collect() should contain theSameElementsAs Seq(
      VariantOutput(
        hmb_ac = 3,
        hmb_an = 4,
        hmb_af = 0.75,
        hmb_homozygotes = 1,
        hmb_heterozygotes = 1,
        gru_ac = 2,
        gru_an = 4,
        gru_af = 0.5,
        gru_homozygotes = 0,
        gru_heterozygotes = 2)
    )
  }

  //Array(
  // VariantOutput(2,165310406,165310406,G,A,chr2:g.166166916G>A,Some(rs1057520413),3,4,0.75,1,1,2,4,0.5,0,2,SNV,SD_123456,RE_ABCDEF))
  // VariantOutput(2,165310406,165310406,G,A,chr2:g.166166916G>A,Some(rs1057520413),4,3,0.75,1,1,4,2,0.5,0,2,SNV,SD_123456,RE_ABCDEF)
  // did not contain the same elements as
  // List(
  // VariantOutput(2,165310406,165310406,G,A,chr2:g.166166916G>A,Some(rs1057520413),4,3,0.75,1,1,4,2,0.5,0,2,SNV,SD_123456,RE_ABCDEF))

}
