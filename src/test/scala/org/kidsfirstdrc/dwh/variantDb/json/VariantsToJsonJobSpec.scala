package org.kidsfirstdrc.dwh.variantDb.json

import org.apache.spark.sql.DataFrame
import org.kidsfirstdrc.dwh.conf.Catalog.{Clinical, Public}
import org.kidsfirstdrc.dwh.testutils.Model.{Exon, Freq, JoinConsequenceOutput, JoinVariantOutput, RefAlt, ThousandGenomesFreq}
import org.kidsfirstdrc.dwh.testutils.VariantToJsonJobModel._
import org.kidsfirstdrc.dwh.testutils.external.{CosmicCancerGeneCensusOutput, DddGeneCensusOutput, Omim, OrphanetOutput}
import org.kidsfirstdrc.dwh.testutils.{VariantToJsonJobModel, WithSparkSession}
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class VariantsToJsonJobSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {
  import spark.implicits._
  val studyId1 = "SD_123"
  val studyId2 = "SD_456"
  val studyId3 = "SD_789"
  val realeaseId = "RE_ABCDEF"

  val joinVariantDf: DataFrame = Seq(
    JoinVariantOutput(
      hmb_ac = 12, hmb_an = 27, hmb_af = 0.4444444444, hmb_homozygotes = 9, hmb_heterozygotes = 7,
      gru_ac = 2, gru_an = 7, gru_af = 0.2857142857, gru_homozygotes = 5, gru_heterozygotes = 1,
      hmb_ac_by_study = Map(studyId1 -> 5, studyId2 -> 5, studyId3 -> 2),
      hmb_an_by_study = Map(studyId1 -> 10, studyId2 -> 10, studyId3 -> 7),
      hmb_af_by_study = Map(studyId1 -> 0.5, studyId2 -> 0.5, studyId3 -> 0.2857142857),
      hmb_homozygotes_by_study = Map(studyId1 -> 2, studyId2 -> 2, studyId3 -> 5),
      hmb_heterozygotes_by_study = Map(studyId1 -> 3, studyId2 -> 3, studyId3 -> 1),
      gru_ac_by_study = Map(studyId1 -> 0, studyId2 -> 0, studyId3 -> 2),
      gru_an_by_study = Map(studyId1 -> 0, studyId2 -> 0, studyId3 -> 7),
      gru_af_by_study = Map(studyId1 -> 0, studyId2 -> 0, studyId3 -> 0.2857142857),
      gru_homozygotes_by_study = Map(studyId1 -> 0, studyId2 -> 0, studyId3 -> 5),
      gru_heterozygotes_by_study = Map(studyId1 -> 0, studyId2 -> 0, studyId3 -> 1),
      studies = Set(studyId1, studyId2, studyId3),
      consent_codes = Set("SD_789.c99", "SD_123.c1", "SD_456.c1"),
      consent_codes_by_study = Map(studyId1 -> Set("SD_123.c1"), studyId2 -> Set("SD_456.c1"), studyId3 -> Set(s"$studyId3.c99")),
      release_id = realeaseId,
      clin_sig = Some("pathogenic"),
      clinvar_id = Some("RCV000436956"))
  ).toDF().withColumnRenamed("one_k_genomes", "1k_genomes")

  val joinConsequencesDf: DataFrame = Seq(
    JoinConsequenceOutput().copy(ensembl_gene_id = "ENSG00000189337", ensembl_transcript_id = Some("ENST00000636564")),
    JoinConsequenceOutput().copy(ensembl_gene_id = "ENSG00000189337", ensembl_transcript_id = Some("ENST00000636203"))
  ).toDF()

  val ominDf: DataFrame = Seq(
    Omim.Output(),
    Omim.Output().copy(ensembl_gene_id = "ENSG00000189337")
  ).toDF()

  val orphanetDf: DataFrame = Seq(
    OrphanetOutput(),
    OrphanetOutput().copy(gene_symbol = "SCN2A")
  ).toDF()

  val dddDf: DataFrame = Seq(
  DddGeneCensusOutput(`symbol` = "SCN2A")
  ).toDF()

  val cosmicDf: DataFrame = Seq(
    CosmicCancerGeneCensusOutput(`symbol` = "SCN2A")
  ).toDF()

  val data = Map(
    Clinical.variants -> joinVariantDf,
    Clinical.consequences -> joinConsequencesDf,
    Public.omim_gene_set -> ominDf,
    Public.orphanet_gene_set -> orphanetDf,
    Public.ddd_gene_set -> dddDf,
    Public.cosmic_gene_set -> cosmicDf
  )

  val expectedStudies = List(
    Study("SD_456", List("SD_456.c1"), List("SD_456"), StudyFrequency(Freq(10,5,0.5,2,3),Freq(0,0,0,0,0)),5),
    Study("SD_789", List("SD_789.c99"), List("SD_789"), StudyFrequency(Freq(7,2,0.2857142857,5,1),Freq(7,2,0.2857142857,5,1)),12),
    Study("SD_123", List("SD_123.c1"), List("SD_123"), StudyFrequency(Freq(10,5,0.5,2,3),Freq(0,0,0,0,0)),5)
  )

  val expectedConsequences: List[Consequence] = List(
    Consequence("MODERATE",Some("ENST00000636203"),None, Some("ENST00000283256.10:c.781G>A"),Some("ENSP00000283256.6:p.Val261Met"),"Transcript",List("missense_variant"),Some("protein_coding"),"SNV",1,Some(Exon(7,27)),None,Some(937),Some(781),Some(RefAlt("V","M")),Some(RefAlt("GTG","ATG")),Some(261),Some("V261M"),Some("781G>A"),3,true,
      ConsequenceScore(ScoreConservations(0.5),ScorePredictions(0.1,"SIFT_pred",0.2,"HVAR_pred","FATHMM_rankscore","FATHMM_pred","CADD_raw_rankscore","DANN_rankscore",0.3,0.4,"LRT_pred"))),
    Consequence("MODERATE",Some("ENST00000636564"),None, Some("ENST00000283256.10:c.781G>A"),Some("ENSP00000283256.6:p.Val261Met"),"Transcript",List("missense_variant"),Some("protein_coding"),"SNV",1,Some(Exon(7,27)),None,Some(937),Some(781),Some(RefAlt("V","M")),Some(RefAlt("GTG","ATG")),Some(261),Some("V261M"),Some("781G>A"),3,true,
      ConsequenceScore(ScoreConservations(0.5),ScorePredictions(0.1,"SIFT_pred",0.2,"HVAR_pred","FATHMM_rankscore","FATHMM_pred","CADD_raw_rankscore","DANN_rankscore",0.3,0.4,"LRT_pred")))

  )

  "VariantDbJson" should "transform data to the right format" in {

    val result = new VariantsToJsonJob(realeaseId).transform(data)

    val parsedResult = result.as[VariantToJsonJobModel.Output].collect()
    val `1k_genomes`: ThousandGenomesFreq =
      result.select(
        "frequencies.1k_genomes.an",
        "frequencies.1k_genomes.ac",
        "frequencies.1k_genomes.af")
      .as[ThousandGenomesFreq].collect().head
    val variant = parsedResult.head

    //1. make sure we have only 1 row in the result
    parsedResult.length shouldBe 1
    //2. data validation of that row
    variant shouldBe VariantToJsonJobModel.Output(studies = expectedStudies, consequences = expectedConsequences)
    `1k_genomes` shouldBe ThousandGenomesFreq()

  }
}