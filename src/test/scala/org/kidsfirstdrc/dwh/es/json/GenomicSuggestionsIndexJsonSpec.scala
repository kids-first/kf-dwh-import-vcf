package org.kidsfirstdrc.dwh.es.json

import bio.ferlab.datalake.core.config.Configuration
import org.apache.spark.sql.DataFrame
import org.kidsfirstdrc.dwh.conf.Catalog.{Clinical, Public}
import org.kidsfirstdrc.dwh.testutils._
import org.kidsfirstdrc.dwh.testutils.es.{SUGGEST, SuggesterIndexOutput}
import org.kidsfirstdrc.dwh.testutils.external.GenesOutput
import org.kidsfirstdrc.dwh.testutils.join.{Freq, JoinConsequenceOutput, JoinVariantOutput}
import org.kidsfirstdrc.dwh.testutils.vcf.{OccurrenceOutput, VariantFrequency}
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class GenomicSuggestionsIndexJsonSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {
  import spark.implicits._
  val studyId1 = "SD_123"
  val studyId2 = "SD_456"
  val studyId3 = "SD_789"
  val realeaseId = "RE_ABCDEF"

  val occurrencesDf: DataFrame = Seq(
    OccurrenceOutput(chromosome = "3", `start` = 165310406, `end` = 165310406, is_gru = false, is_hmb = false),
    OccurrenceOutput(chromosome = "2", `start` = 165310406, `end` = 165310406, is_gru = true, is_hmb = false)
  ).toDF()

  val variant =
    JoinVariantOutput(
      frequencies = VariantFrequency(Freq(ac = 12, an = 27, af = 0.4444444444, homozygotes = 9, heterozygotes = 7), Freq(ac = 12, an = 27, af = 0.4444444444, homozygotes = 9, heterozygotes = 7)),
      upper_bound_kf_ac_by_study = Map(studyId1 -> 5, studyId2 -> 5, studyId3 -> 2),
      upper_bound_kf_an_by_study = Map(studyId1 -> 10, studyId2 -> 10, studyId3 -> 7),
      upper_bound_kf_af_by_study = Map(studyId1 -> 0.5, studyId2 -> 0.5, studyId3 -> 0.2857142857),
      upper_bound_kf_homozygotes_by_study = Map(studyId1 -> 2, studyId2 -> 2, studyId3 -> 5),
      upper_bound_kf_heterozygotes_by_study = Map(studyId1 -> 3, studyId2 -> 3, studyId3 -> 1),
      lower_bound_kf_ac_by_study = Map(studyId1 -> 0, studyId2 -> 0, studyId3 -> 2),
      lower_bound_kf_an_by_study = Map(studyId1 -> 0, studyId2 -> 0, studyId3 -> 7),
      lower_bound_kf_af_by_study = Map(studyId1 -> 0, studyId2 -> 0, studyId3 -> 0.2857142857),
      lower_bound_kf_homozygotes_by_study = Map(studyId1 -> 0, studyId2 -> 0, studyId3 -> 5),
      lower_bound_kf_heterozygotes_by_study = Map(studyId1 -> 0, studyId2 -> 0, studyId3 -> 1),
      studies = Set(studyId1, studyId2, studyId3),
      consent_codes = Set("SD_789.c99", "SD_123.c1", "SD_456.c1"),
      consent_codes_by_study = Map(studyId1 -> Set("SD_123.c1"), studyId2 -> Set("SD_456.c1"), studyId3 -> Set(s"$studyId3.c99")),
      release_id = realeaseId,
      clin_sig = Some("pathogenic"),
      clinvar_id = Some("RCV000436956"),
      name = "rs1313905795")

  val joinVariantDf: DataFrame = Seq(
    variant,                       //should be in the index
    //variant.copy(chromosome = "3") //should not be in the index
  ).toDF().withColumnRenamed("one_thousand_genomes", "1k_genomes")

  val joinConsequencesDf: DataFrame = Seq(
    JoinConsequenceOutput().copy(symbol = "SCN2A.2", aa_change = Some("V261M")),
    JoinConsequenceOutput().copy(symbol = "SCN2A", aa_change = Some("V261E"))
  ).toDF()

  val joinVariantWithNullDf: DataFrame = Seq(
    variant.copy(hgvsg = null)
  ).toDF().withColumnRenamed("one_thousand_genomes", "1k_genomes")

  val joinConsequencesWithEmptyAndNullDf: DataFrame = Seq(
    JoinConsequenceOutput().copy(symbol = "SCN2A.2", aa_change = None),
    JoinConsequenceOutput().copy(symbol = "SCN2A", aa_change = null),
    JoinConsequenceOutput().copy(symbol = "", aa_change = None),
    JoinConsequenceOutput().copy(symbol = null, aa_change = null)
  ).toDF()

  val genesDf: DataFrame = Seq(
    GenesOutput(
      `alias` = List("BII", "CACH6", "CACNL1A6", "Cav2.3", "", null)
    )
  ).toDF()

  val data = Map(
    Public.genes -> genesDf,
    Clinical.occurrences -> occurrencesDf,
    Clinical.variants -> joinVariantDf,
    Clinical.consequences -> joinConsequencesDf
  )

  implicit val conf: Configuration =  Configuration(List())

  "suggester index job" should "transform data to the right format" in {

    val result = new GenomicSuggestionsIndexJson("re_000010").transform(data)
    result.show(false)

    result.as[SuggesterIndexOutput].collect() should contain allElementsOf Seq(
      SuggesterIndexOutput(),
      SuggesterIndexOutput(
        `type` = "gene",
        `chromosome` = null,
        `locus` = null,
        `suggestion_id` = "9b8016c31b93a7504a8314ce3d060792f67ca2ad",
        `hgvsg` = null,
        `symbol` = "OR4F5",
        `rsnumber` = null,
        `suggest` = List(
          SUGGEST(List("OR4F5"), 5),
          SUGGEST(List("BII", "CACH6", "CACNL1A6", "Cav2.3", "ENSG00000198216"), 3)))
    )
  }

  "suggester from variants" should "remove null and empty values" in {

    val result = new GenomicSuggestionsIndexJson("").getVariantSuggest(joinVariantWithNullDf, joinConsequencesWithEmptyAndNullDf)
    result.show(false)

    result.repartition(1)
      .write.mode("overwrite")
      .json(getClass.getClassLoader.getResource(".").getFile + "test_result")

    val expectedResult = SuggesterIndexOutput(
      `hgvsg` = "",
      `suggest` = List(
        SUGGEST(List("SCN2A", "SCN2A.2", "2-165310406-G-A", "rs1313905795", "RCV000436956"), 4),
        SUGGEST(List("SCN2A", "SCN2A.2", "ENSG00000136531", "ENST00000486878"), 2)))

    result.as[SuggesterIndexOutput].collect() should contain allElementsOf Seq(
      expectedResult
    )
  }
}