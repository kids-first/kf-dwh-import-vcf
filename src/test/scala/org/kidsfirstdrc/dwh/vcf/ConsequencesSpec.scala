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
      ConsequencesRowInput(
        annotations = Seq(
          ConsequenceInput(),
          ConsequenceInput(Consequence  = Seq("missense_variant", "NMD_transcript_variant"), Feature = "ENST00000636135.1")
        )
      ),
      ConsequencesRowInput(
        annotations = Seq(
          ConsequenceInput()
        )
      )
    ).toDF()

    val output = Consequences.build(studyId, releaseId, df)
    output.as[ConsequenceOutput].collect() should contain theSameElementsAs Seq(
      ConsequenceOutput(),
      ConsequenceOutput(ensembl_transcript_id = Some("ENST00000636135.1"), consequences = Seq("missense_variant", "NMD_transcript_variant"))
    )
  }

  it should "return a dataframe with one ConsequenceInput with regulatory feature" in {
    val df = Seq(
      ConsequencesRowInput(
        annotations = Seq(
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

//Array(
// ConsequenceOutput(2,165310406,165310406,G,[A],SCN2A,MODERATE,ENSG00000136531,Some(ENST00000283256.10),None,Transcript,missense_variant,Some(protein_coding),Some([rs1057520413]),SNV,1,Some(chr2:g.166166916G>A),Some(ENST00000283256.10:c.781G>A),Some(ENSP00000283256.6:p.Val261Met),Some(Exon(7,27)),None,Some(937),Some(781),Some(RefAlt(V,M)),Some(RefAlt(GTG,ATG)),Some(261),SD_123456,RE_ABCDEF),
// ConsequenceOutput(2,165310406,165310406,G,A,SCN2A,MODERATE,ENSG00000136531,Some(ENST00000283256.10),None,Transcript,missense_variant,Some(protein_coding),Some(rs1057520413),SNV,1,Some(chr2:g.166166916G>A),Some(ENST00000283256.10:c.781G>A),Some(ENSP00000283256.6:p.Val261Met),Some(Exon(7,27)),None,Some(937),Some(781),Some(RefAlt(V,M)),Some(RefAlt(GTG,ATG)),Some(261),SD_123456,RE_ABCDEF)
//
// ConsequenceOutput(2,165310406,165310406,G,[A],SCN2A,MODERATE,ENSG00000136531,Some(ENST00000636135.1),None,Transcript,missense_variant,Some(protein_coding),Some([rs1057520413]),SNV,1,Some(chr2:g.166166916G>A),Some(ENST00000283256.10:c.781G>A),Some(ENSP00000283256.6:p.Val261Met),Some(Exon(7,27)),None,Some(937),Some(781),Some(RefAlt(V,M)),Some(RefAlt(GTG,ATG)),Some(261),SD_123456,RE_ABCDEF),
// ConsequenceOutput(2,165310406,165310406,G,[A],SCN2A,MODERATE,ENSG00000136531,Some(ENST00000636135.1),None,Transcript,NMD_transcript_variant,Some(protein_coding),Some([rs1057520413]),SNV,1,Some(chr2:g.166166916G>A),Some(ENST00000283256.10:c.781G>A),Some(ENSP00000283256.6:p.Val261Met),Some(Exon(7,27)),None,Some(937),Some(781),Some(RefAlt(V,M)),Some(RefAlt(GTG,ATG)),Some(261),SD_123456,RE_ABCDEF)) did not contain the same elements
// as List(
// ConsequenceOutput(2,165310406,165310406,G,A,SCN2A,MODERATE,ENSG00000136531,Some(ENST00000283256.10),None,Transcript,missense_variant,Some(protein_coding),Some(rs1057520413),SNV,1,Some(chr2:g.166166916G>A),Some(ENST00000283256.10:c.781G>A),Some(ENSP00000283256.6:p.Val261Met),Some(Exon(7,27)),None,Some(937),Some(781),Some(RefAlt(V,M)),Some(RefAlt(GTG,ATG)),Some(261),SD_123456,RE_ABCDEF),
// ConsequenceOutput(2,165310406,165310406,G,A,SCN2A,MODERATE,ENSG00000136531,Some(ENST00000636135.1),None,Transcript,missense_variant,Some(protein_coding),Some(rs1057520413),SNV,1,Some(chr2:g.166166916G>A),Some(ENST00000283256.10:c.781G>A),Some(ENSP00000283256.6:p.Val261Met),Some(Exon(7,27)),None,Some(937),Some(781),Some(RefAlt(V,M)),Some(RefAlt(GTG,ATG)),Some(261),SD_123456,RE_ABCDEF),
// ConsequenceOutput(2,165310406,165310406,G,A,SCN2A,MODERATE,ENSG00000136531,Some(ENST00000636135.1),None,Transcript,NMD_transcript_variant,Some(protein_coding),Some(rs1057520413),SNV,1,Some(chr2:g.166166916G>A),Some(ENST00000283256.10:c.781G>A),Some(ENSP00000283256.6:p.Val261Met),Some(Exon(7,27)),None,Some(937),Some(781),Some(RefAlt(V,M)),Some(RefAlt(GTG,ATG)),Some(261),SD_123456,RE_ABCDEF))
