package org.kidsfirstdrc.dwh.testutils

object Model {


  case class VariantInput(chromosome: String = "2",
                          start: Long = 165310406,
                          end: Long = 165310406,
                          reference: String = "G",
                          alternate: String = "A",
                          hgvsg: String = "chr2:g.166166916G>A",
                          name: Option[String] = Some("rs1057520413"),
                          variant_class: String = "SNV",
                          zygosity: String = "HOM",
                          has_alt: Int = 1,
                          is_hmb: Boolean = true,
                          is_gru: Boolean = false,
                          study_id: String = "SD_123456",
                          release_id: String = "RE_ABCDEF")

  case class VariantOutput(chromosome: String = "2",
                           start: Long = 165310406,
                           end: Long = 165310406,
                           reference: String = "G",
                           alternate: String = "A",
                           hgvsg: String = "chr2:g.166166916G>A",
                           name: Option[String] = Some("rs1057520413"),
                           hmb_ac: Long = 2,
                           hmb_an: Long = 2,
                           hmb_af: BigDecimal = 1,
                           hmb_homozygotes: Long = 1,
                           hmb_heterozygotes: Long = 0,
                           gru_ac: Long = 0,
                           gru_an: Long = 0,
                           gru_af: BigDecimal = 0,
                           gru_homozygotes: Long = 0,
                           gru_heterozygotes: Long = 0,
                           variant_class: String = "SNV",
                           study_id: String = "SD_123456",
                           release_id: String = "RE_ABCDEF")

  case class OccurrencesOutput(chromosome: String,
                               start: Long,
                               end: Long,
                               reference: String,
                               alternate: String,
                               name: Option[String],
                               biospecimen_id: String,
                               participant_id: String,
                               family_id: Option[String],
                               study_id: String,
                               release_id: String,
                               file_name: String,
                               dbgap_consent_code: String)

  case class ConsequencesRowInput(chromosome: String = "2",
                                  start: Long = 165310406,
                                  end: Long = 165310406,
                                  reference: String = "G",
                                  alternate: String = "A",
                                  name: String = "rs1057520413",
                                  annotations: Seq[ConsequenceInput] = Seq(ConsequenceInput()),
                                  splitFromMultiAllelic: Boolean = false)

  case class Genotype(calls: Array[Int])

  val hom_00: Genotype = Genotype(Array(0, 0))
  val hom_11: Genotype = Genotype(Array(1, 1))
  val het_01: Genotype = Genotype(Array(0, 1))
  val het_10: Genotype = Genotype(Array(1, 0))

  val unk: Genotype = Genotype(Array(-1, 0))

  case class Exon(rank: Int, total: Int)

  case class Intron(rank: Int, total: Int)

  case class RefAlt(reference: String, variant: String)

  case class ConsequenceOutput(chromosome: String = "2",
                               start: Long = 165310406,
                               end: Long = 165310406,
                               reference: String = "G",
                               alternate: String = "A",
                               symbol: String = "SCN2A",
                               impact: String = "MODERATE",
                               ensembl_gene_id: String = "ENSG00000136531",
                               ensembl_transcript_id: Option[String] = Some("ENST00000283256.10"),
                               ensembl_regulatory_id: Option[String] = None,
                               feature_type: String = "Transcript",
                               consequence: String = "missense_variant",
                               biotype: Option[String] = Some("protein_coding"),
                               name: Option[String] = Some("rs1057520413"),
                               variant_class: String = "SNV",
                               strand: Int = 1,
                               hgvsg: Option[String] = Some("chr2:g.166166916G>A"),
                               hgvsc: Option[String] = Some("ENST00000283256.10:c.781G>A"),
                               hgvsp: Option[String] = Some("ENSP00000283256.6:p.Val261Met"),
                               exon: Option[Exon] = Some(Exon(7, 27)),
                               intron: Option[Intron] = None,
                               cdna_position: Option[Int] = Some(937),
                               cds_position: Option[Int] = Some(781),
                               amino_acids: Option[RefAlt] = Some(RefAlt("V", "M")),
                               codons: Option[RefAlt] = Some(RefAlt("GTG", "ATG")),
                               protein_position: Option[Int] = Some(261),
                               study_id: String = "SD_123456",
                               release_id: String = "RE_ABCDEF")


  case class ConsequenceInput(
                               Allele: String = "A",
                               Consequence: Seq[String] = Seq("missense_variant"),
                               SYMBOL: String = "SCN2A",
                               Gene: String = "ENSG00000136531",
                               Feature: String = "ENST00000283256.10",
                               IMPACT: String = "MODERATE",
                               BIOTYPE: String = "protein_coding",
                               Feature_type: String = "Transcript",
                               STRAND: Int = 1,
                               VARIANT_CLASS: String = "SNV",
                               HGVSg: String = "chr2:g.166166916G>A",
                               HGVSc: String = "ENST00000283256.10:c.781G>A",
                               HGVSp: String = "ENSP00000283256.6:p.Val261Met",
                               cDNA_position: Option[Int] = Some(937),
                               CDS_position: Option[Int] = Some(781),
                               Amino_acids: Option[RefAlt] = Some(RefAlt("V", "M")),
                               Protein_position: Option[Int] = Some(261),
                               Codons: Option[RefAlt] = Some(RefAlt("GTG", "ATG")),
                               EXON: Option[Exon] = Some(Exon(7, 27)),
                               INTRON: Option[Intron] = None
                             )

  case class JoinVariantOutput(chromosome: String = "2",
                               start: Long = 165310406,
                               end: Long = 165310406,
                               reference: String = "G",
                               alternate: String = "A",
                               name: String = "rs1057520413",
                               hgvsg: String = "chr2:g.166166916G>A",
                               variant_class: String = "SNV",
                               hmb_ac: Long = 1,
                               hmb_an: Long = 2,
                               hmb_af: BigDecimal = 0.5,
                               hmb_homozygotes: Long = 1,
                               hmb_heterozygotes: Long = 1,
                               gru_ac: Long = 0,
                               gru_an: Long = 0,
                               gru_af: BigDecimal = 0,
                               gru_homozygotes: Long = 0,
                               gru_heterozygotes: Long = 0,
                               topmed: Option[Freq] = Some(Freq(20, 10, 0.5, 10, 10)),
                               gnomad_genomes_2_1: Option[Freq] = Some(Freq(20, 10, 0.5, 10, 10)),
                               clinvar_id: Option[String] = Some("RCV000436956"),
                               clin_sig: Option[String] = Some("Pathogenic"),
                               dbsnp_id: Option[String] = Some("rs1234567"),
                               hmb_ac_by_study: Map[String, Long],
                               hmb_an_by_study: Map[String, Long],
                               hmb_af_by_study: Map[String, BigDecimal],
                               hmb_homozygotes_by_study: Map[String, Long],
                               hmb_heterozygotes_by_study: Map[String, Long],
                               gru_ac_by_study: Map[String, Long],
                               gru_an_by_study: Map[String, Long],
                               gru_af_by_study: Map[String, BigDecimal],
                               gru_homozygotes_by_study: Map[String, Long],
                               gru_heterozygotes_by_study: Map[String, Long],
                               studies: Set[String],
                               release_id: String = "RE_ABCDEF"
                              )

  case class JoinConsequenceOutput(chromosome: String = "2",
                                   start: Long = 165310406,
                                   end: Long = 165310406,
                                   reference: String = "G",
                                   alternate: String = "A",
                                   symbol: String = "SCN2A",
                                   impact: String = "MODERATE",
                                   ensembl_gene_id: String = "ENSG00000136531",
                                   ensembl_transcript_id: Option[String] = Some("ENST00000283256.10"),
                                   ensembl_regulatory_id: Option[String] = None,
                                   feature_type: String = "Transcript",
                                   consequence: String = "missense_variant",
                                   biotype: Option[String] = Some("protein_coding"),
                                   name: Option[String] = Some("rs1057520413"),
                                   variant_class: String = "SNV",
                                   strand: Int = 1,
                                   hgvsg: Option[String] = Some("chr2:g.166166916G>A"),
                                   hgvsc: Option[String] = Some("ENST00000283256.10:c.781G>A"),
                                   hgvsp: Option[String] = Some("ENSP00000283256.6:p.Val261Met"),
                                   exon: Option[Exon] = Some(Exon(7, 27)),
                                   intron: Option[Intron] = None,
                                   cdna_position: Option[Int] = Some(937),
                                   cds_position: Option[Int] = Some(781),
                                   amino_acids: Option[RefAlt] = Some(RefAlt("V", "M")),
                                   codons: Option[RefAlt] = Some(RefAlt("GTG", "ATG")),
                                   protein_position: Option[Int] = Some(261),
                                   study_ids: Set[String] = Set("SD_123456"),
                                   release_id: String = "RE_ABCDEF",
                                   sift_score: Option[Double] = Some(0.99),
                                   aa_change: Option[String] = Some("V261M"),
                                   coding_dna_change: Option[String] = Some("781G>A")

                                  )

  case class DBSNFPScore(chromosome: String = "2",
                         start: Long = 165310406,
                         reference: String = "G",
                         alternate: String = "A",
                         symbol: String = "SCN2A",
                         ensembl_gene_id: String = "ENSG00000136531",
                         ensembl_transcript_id: Option[String] = Some("ENST00000283256.10"),
                         ensembl_protein_id: Option[String] = None,
                         cds_strand: Option[Int] = None,
                         sift_score: Option[Double] = Some(0.99)
                        )

  case class Freq(an: Long, ac: Long, af: BigDecimal, homozygotes: Long, heterozygotes: Long)

  case class FrequencyEntry(chromosome: String = "2",
                            start: Long = 165310406,
                            end: Long = 165310406,
                            reference: String = "G",
                            alternate: String = "A",
                            ac: Long = 10,
                            an: Long = 20,
                            af: BigDecimal = 0.5, homozygotes: Long = 10, heterozygotes: Long = 10)

  case class ClinvarEntry(chromosome: String = "2",
                          start: Long = 165310406,
                          end: Long = 165310406,
                          reference: String = "G",
                          alternate: String = "A",
                          name: String = "RCV000436956",
                          clin_sig: String = "Pathogenic"
                         )

  case class DBSNPEntry(chromosome: String = "2",
                        start: Long = 165310406,
                        end: Long = 165310406,
                        reference: String = "G",
                        alternate: String = "A",
                        name: String = "rs1234567"
                       )

  case class ParticipantInput(kf_id: String = "PT_001", family_id: String = "FM_001", affected_status: String = "alive")

  case class BiospecimenInput(kf_id: String = "BS_0001", participant_id: String = "PT_001")

  case class ParticipantOutput(kf_id: String = "PT_001", family_id: String = "FM_001", affected_status: String = "alive", study_id: String = "SD_123", release_id: String = "RE_ABCDEF")

  case class BiosepecimenOutput(kf_id: String = "BS_0001", biospecimen_id: String = "BS_0001", participant_id: String = "PT_001", family_id: String = "FM_001", study_id: String = "SD_123", release_id: String = "RE_ABCDEF")

}
