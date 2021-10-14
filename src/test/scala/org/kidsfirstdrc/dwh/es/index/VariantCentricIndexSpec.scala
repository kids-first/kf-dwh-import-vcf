package org.kidsfirstdrc.dwh.es.index

import bio.ferlab.datalake.commons.config.{Configuration, StorageConf}
import org.apache.spark.sql.DataFrame
import org.kidsfirstdrc.dwh.conf.Catalog.{Clinical, Public, Raw}
import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.kidsfirstdrc.dwh.testutils.dataservice.StudyShortNameRaw
import org.kidsfirstdrc.dwh.testutils.es.VariantCentricOutput
import org.kidsfirstdrc.dwh.testutils.es.VariantCentricOutput._
import org.kidsfirstdrc.dwh.testutils.external.{ClinvarOutput, GenesOutput}
import org.kidsfirstdrc.dwh.testutils.join.{Freq, JoinConsequenceOutput, JoinVariantOutput}
import org.kidsfirstdrc.dwh.testutils.vcf._
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class VariantCentricIndexSpec
    extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {
  import spark.implicits._
  val studyId1 = "SD_123"
  val studyId2 = "SD_456"
  val studyId3 = "SD_789"
  val studyId4 = "SD_999"

  val realeaseId = "RE_ABCDEF"

  implicit val conf: Configuration =
    Configuration(
      List(StorageConf("kf-strides-variant", getClass.getClassLoader.getResource(".").getFile))
    )

  val studyShortNamesDf: DataFrame = Seq(
    StudyShortNameRaw("SD_123", "A KF study", "KF-ABC"),
    StudyShortNameRaw("SD_456", "A KF study", "KF-DEF"),
    StudyShortNameRaw("SD_789", "A KF study", "KF-GHI")
  ).toDF

  val joinVariantDf: DataFrame = Seq(
    JoinVariantOutput( // should be kept in result
      frequencies = VariantFrequency(
        Freq(ac = 12, an = 30, af = 0.4, homozygotes = 9, heterozygotes = 7),
        Freq(ac = 12, an = 27, af = 0.4444444444, homozygotes = 9, heterozygotes = 7)
      ),
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
      consent_codes_by_study = Map(
        studyId1 -> Set("SD_123.c1"),
        studyId2 -> Set("SD_456.c1"),
        studyId3 -> Set(s"$studyId3.c99")
      ),
      zygosity = List("HOM", "HET", "WT", "UNK"),
      release_id = realeaseId,
      clin_sig = Some("pathogenic"),
      clinvar_id = Some("RCV000436956"),
      transmissions = Map("autosomal_dominant" -> 13, "autosomal_recessive" -> 1, "non_carrier_proband" -> 900),
      transmissions_by_study = Map(studyId1 -> Map("autosomal_dominant" -> 11, "autosomal_recessive" -> 1), studyId2 -> Map("autosomal_dominant" -> 1), studyId3 -> Map("autosomal_dominant" -> 1))
    ),
    JoinVariantOutput( // should be filtered out
      hgvsg = null,
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
      consent_codes_by_study = Map(
        studyId1 -> Set("SD_123.c1"),
        studyId2 -> Set("SD_456.c1"),
        studyId3 -> Set(s"$studyId3.c99")
      ),
      transmissions = Map("AD" -> 13, "AR" -> 1),
      transmissions_by_study = Map(studyId1 -> Map("AD" -> 11, "AR" -> 1), studyId2 -> Map("AD" -> 1), studyId3 -> Map("AD" -> 1))
    )
  ).toDF().withColumnRenamed("one_thousand_genomes", "1k_genomes")

  val joinConsequencesDf: DataFrame = Seq(
    JoinConsequenceOutput().copy(
      `impact` = "MODERATE",
      `ensembl_gene_id` = "ENSG00000189337",
      `ensembl_transcript_id` = "ENST00000636564",
      `ensembl_regulatory_id` = Some("ENSR0000636135"),
      `intron` = Some(Intron(2, 10)),
      `SIFT_score` = Some(0.91255),
      `SIFT_pred` = Some("D"),
      `Polyphen2_HVAR_pred` = Some("D"),
      `Polyphen2_HVAR_score` = Some(0.91255),
      `FATHMM_pred` = Some("D"),
      `mane_plus` = Some(true),
      `refseq_mrna_id` = Some("MN_XXX")
    ),
    JoinConsequenceOutput().copy(
      `impact` = "LOW",
      `ensembl_gene_id` = "ENSG00000189337",
      `ensembl_transcript_id` = "ENST00000636203",
      `ensembl_regulatory_id` = Some("ENSR0000636134"),
      `intron` = Some(Intron(2, 10)),
      `SIFT_score` = Some(0.91255),
      `SIFT_pred` = Some("D"),
      `Polyphen2_HVAR_pred` = Some("D"),
      `Polyphen2_HVAR_score` = Some(0.91255),
      `FATHMM_pred` = Some("D")
    )
  ).toDF()

  val occurrencesDf: DataFrame = Seq(
    OccurrenceOutput(
      `start` = 165310406,
      `end` = 165310406,
      `has_alt` = 0,
      participant_id = "PT_000001"
    ), //removed because `has_alt` = 0
    OccurrenceOutput(
      `start` = 165310406,
      `end` = 165310406,
      `has_alt` = 1,
      participant_id = "PT_000002",
      `study_id` = "SD_123"
    ),
    OccurrenceOutput(
      `start` = 165310406,
      `end` = 165310406,
      `has_alt` = 1,
      participant_id = "PT_000003",
      `study_id` = "SD_123"
    ),
    OccurrenceOutput(
      `start` = 165310406,
      `end` = 165310406,
      `has_alt` = 1,
      participant_id = "PT_000004",
      `study_id` = "SD_123"
    ),
    OccurrenceOutput(
      `start` = 165310406,
      `end` = 165310406,
      `has_alt` = 1,
      participant_id = "PT_000005",
      `study_id` = "SD_123"
    ),
    OccurrenceOutput(
      `start` = 165310406,
      `end` = 165310406,
      `has_alt` = 1,
      participant_id = "PT_000006",
      `study_id` = "SD_123"
    ),
    OccurrenceOutput(
      `start` = 165310406,
      `end` = 165310406,
      `has_alt` = 1,
      participant_id = "PT_000007",
      `study_id` = "SD_123"
    ),
    OccurrenceOutput(
      `start` = 165310406,
      `end` = 165310406,
      `has_alt` = 1,
      participant_id = "PT_000008",
      `study_id` = "SD_123"
    ),
    OccurrenceOutput(
      `start` = 165310406,
      `end` = 165310406,
      `has_alt` = 1,
      participant_id = "PT_000009",
      `study_id` = "SD_123"
    ),
    OccurrenceOutput(
      `start` = 165310406,
      `end` = 165310406,
      `has_alt` = 1,
      participant_id = "PT_000010",
      `study_id` = "SD_123"
    ),
    OccurrenceOutput(
      `start` = 165310406,
      `end` = 165310406,
      `has_alt` = 1,
      participant_id = "PT_000011",
      `study_id` = "SD_123"
    ),
    OccurrenceOutput(
      `start` = 165310406,
      `end` = 165310406,
      `has_alt` = 1,
      participant_id = "PT_000012",
      `study_id` = "SD_123"
    ),
    OccurrenceOutput(
      `start` = 165310406,
      `end` = 165310406,
      `has_alt` = 1,
      participant_id = "PT_000033",
      `study_id` = "SD_456"
    ) //removed because <10 participants
  ).toDF()

  val clinvarDf: DataFrame = Seq(
    ClinvarOutput().copy(start = 165310406, end = 165310406, reference = "G", alternate = "A")
  ).toDF()

  val genesDf: DataFrame = Seq(
    GenesOutput(`chromosome` = "2", `symbol` = "SCN2A", `ensembl_gene_id` = "ENSG00000189337")
  ).toDF()

  val data = Map(
    Clinical.variants.id      -> joinVariantDf,
    Clinical.consequences.id  -> joinConsequencesDf,
    Clinical.occurrences.id   -> occurrencesDf,
    Public.clinvar.id         -> clinvarDf,
    Public.genes.id           -> genesDf,
    Raw.studies_short_name.id -> studyShortNamesDf
  )

  val expectedStudies = List(
    Study(
      "SD_456",
      "KF-DEF",
      List("SD_456.c1"),
      List("SD_456"),
      StudyFrequency(Freq(10, 5, 0.5, 2, 3), Freq(0, 0, 0, 0, 0)),
      1,
      null,
      List("autosomal_dominant")
    ),
    Study(
      "SD_123",
      "KF-ABC",
      List("SD_123.c1"),
      List("SD_123"),
      StudyFrequency(Freq(10, 5, 0.5, 2, 3), Freq(0, 0, 0, 0, 0)),
      11,
      List(
        "PT_000002",
        "PT_000003",
        "PT_000004",
        "PT_000005",
        "PT_000006",
        "PT_000007",
        "PT_000008",
        "PT_000009",
        "PT_000010",
        "PT_000011",
        "PT_000012"
      ),
      List("autosomal_dominant", "autosomal_recessive")
    ),
    Study(
      "SD_789",
      "KF-GHI",
      List("SD_789.c99"),
      List("SD_789"),
      StudyFrequency(Freq(7, 2, 0.2857142857, 5, 1), Freq(7, 2, 0.2857142857, 5, 1)),
      0,
      null,
      List("autosomal_dominant")
    )
  )

  val expectedConsequences: List[Consequence] = List(
    Consequence(
      "LOW",
      "SCN2A",
      Some("ENST00000636203"),
      Some("ENSR0000636134"),
      Some("ENST00000486878.2:c.322G>A"),
      Some("ENSP00000487466.1:p.Val108Met"),
      "Transcript",
      List("missense_variant"),
      Some("protein_coding"),
      1,
      Some(Exon(Some(4), Some(4))),
      Some(Intron(2, 10)),
      Some(322),
      Some(322),
      Some(RefAlt("V", "M")),
      Some(RefAlt("Gtg", "Atg")),
      Some(108),
      Some("V108M"),
      Some("322G>A"),
      2,
      false,
      None,
      None,
      None,
      None,
      ScoreConservations(0.7674),
      ScorePredictions()
    ),
    Consequence(
      "MODERATE",
      "SCN2A",
      Some("ENST00000636564"),
      Some("ENSR0000636135"),
      Some("ENST00000486878.2:c.322G>A"),
      Some("ENSP00000487466.1:p.Val108Met"),
      "Transcript",
      List("missense_variant"),
      Some("protein_coding"),
      1,
      Some(Exon(Some(4), Some(4))),
      Some(Intron(2, 10)),
      Some(322),
      Some(322),
      Some(RefAlt("V", "M")),
      Some(RefAlt("Gtg", "Atg")),
      Some(108),
      Some("V108M"),
      Some("322G>A"),
      3,
      false,
      Some(true),
      None,
      Some("MN_XXX"),
      None,
      ScoreConservations(0.7674),
      ScorePredictions()
    )
  )

  val expectedGenes = List(GENES())

  "VariantDbJson" should "transform data to the right format" in {

    val result = new VariantCentricIndex("portal", realeaseId).transform(data)

    val parsedResult = result.as[VariantCentricOutput.Output].collect()
    val variant      = parsedResult.head

    //1. make sure we have only 1 row in the result
    parsedResult.length shouldBe 1
    //2. data validation of that row
    variant.consequences should contain allElementsOf expectedConsequences
    variant.studies should contain allElementsOf expectedStudies
    variant.genes should contain allElementsOf expectedGenes
    variant.`vep_impacts` shouldBe List("LOW", "MODERATE")
    variant.copy(consequences = List(), studies = List(), genes = List()) shouldBe
      VariantCentricOutput.Output(studies = List(), consequences = List(), genes = List())

    result.write
      .mode("overwrite")
      .json(this.getClass.getClassLoader.getResource(".").getFile + "variant_centric")
  }
}
