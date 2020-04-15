package org.kidsfirstdrc.dwh.testutils

object Model {

  case class VariantInput(contigName: String = "chr2",
                          start: Long = 165310405,
                          end: Long = 165310405,
                          referenceAllele: String = "G",
                          alternateAlleles: Seq[String] = Seq("A"),
                          INFO_AC: Seq[Long] = Nil,
                          INFO_AN: Long = 0,
                          names: Seq[String] = Seq("rs1057520413"),
                          INFO_ANN: Seq[ConsequenceInput] = Seq(ConsequenceInput()),
                          genotypes: Seq[Genotype] = Seq(hom_11),
                          splitFromMultiAllelic: Boolean = false
                         )

  case class VariantOutput(chromosome: String = "2",
                           start: Long = 165310405,
                           end: Long = 165310405,
                           reference: String = "G",
                           alternate: String = "A",
                           hgvsg: String = "chr2:g.166166916G>A",
                           name: Option[String] = Some("rs1057520413"),
                           ac: Long = 1,
                           an: Long = 1,
                           af: BigDecimal = 1,
                           variant_class: String = "SNV",
                           homozygotes: Long = 1,
                           heterozygotes: Long = 0,
                           study_id: String = "SD_123456",
                           release_id: String = "RE_ABCDEF")

  case class OccurencesOutput(chromosome: String,
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
                              dbgap_consent_code: String)

  case class Genotype(calls: Array[Int])

  val hom_00: Genotype = Genotype(Array(0, 0))
  val hom_11: Genotype = Genotype(Array(1, 1))
  val het_01: Genotype = Genotype(Array(0, 1))
  val het_10: Genotype = Genotype(Array(1, 0))

  case class Exon(rank: Int, total: Int)

  case class Intron(rank: Int, total: Int)

  case class RefAlt(reference: String, variant: String)

  case class ConsequenceOutput(chromosome: String = "2",
                               start: Long = 165310405,
                               end: Long = 165310405,
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
                               start: Long = 165310405,
                               end: Long = 165310405,
                               reference: String = "G",
                               alternate: String = "A",
                               name: String = "rs1057520413",
                               hgvsg: String = "chr2:g.166166916G>A",
                               variant_class: String = "SNV",
                               ac: Long = 1,
                               an: Long = 2,
                               af: BigDecimal = 0.5,
                               homozygotes: Long = 1,
                               heterozygotes: Long = 1,
                               topmed: Option[Freq] = Some(Freq(20, 10, 0.5, 10, 10)),
                               //                               `1k_genomes`: Freq = Freq(20, 10, 0.5, 10, 10),
                               gnomad_2_1: Option[Freq] = Some(Freq(20, 10, 0.5, 10, 10)),
                               clinvar_id: Option[String] = Some("RCV000436956"),
                               clin_sig: Option[String] = Some("Pathogenic"),
                               by_study: Map[String, Freq],
                               release_id: String = "RE_ABCDEF"
                              )

  case class Freq(an: Long, ac: Long, af: BigDecimal, homozygotes: Long, heterozygotes: Long)

  case class FrequencyEntry(chromosome: String = "2",
                            start: Long = 165310405,
                            end: Long = 165310405,
                            reference: String = "G",
                            alternate: String = "A",
                            ac: Long = 10,
                            an: Long = 20,
                            af: BigDecimal = 0.5, homozygotes: Long = 10, heterozygotes: Long = 10)

  case class ClinvarEntry(chromosome: String = "2",
                          start: Long = 165310405,
                          end: Long = 165310405,
                          reference: String = "G",
                          alternate: String = "A",
                          name: String = "RCV000436956",
                          clin_sig: String = "Pathogenic"
                         )

}
