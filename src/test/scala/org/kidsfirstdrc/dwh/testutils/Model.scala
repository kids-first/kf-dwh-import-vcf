package org.kidsfirstdrc.dwh.testutils

object Model {

  case class VariantInput(contigName: String,
                          start: Long,
                          end: Long,
                          referenceAllele: String,
                          alternateAlleles: Seq[String],
                          INFO_AC: Seq[Long],
                          INFO_AN: Long,
                          names: Seq[String],
                          INFO_ANN: Seq[ConsequenceInput],
                          genotypes: Seq[Genotype],
                          splitFromMultiAllelic: Boolean = false
                         )

  def variant(contigName: String = "chr2",
              start: Long = 165310405,
              end: Long = 165310405,
              referenceAllele: String = "G",
              alternateAlleles: Seq[String] = Seq("A"),
              INFO_AC: Seq[Long] = Nil,
              INFO_AN: Long = 0,
              names: Seq[String] = Seq("rs1057520413"),
              INFO_ANN: Seq[ConsequenceInput] = Seq(consequence()),
              genotype: Seq[Genotype] = Seq(hom_11),
              splitFromMultiAllelic: Boolean = false
             ): VariantInput = VariantInput(contigName, start, end, referenceAllele, alternateAlleles, INFO_AC, INFO_AN, names, INFO_ANN, genotype, splitFromMultiAllelic)

  case class VariantOutput(chromosome: String,
                           start: Long,
                           end: Long,
                           reference: String,
                           alternate: String,
                           hgvsg: String,
                           name: Option[String],
                           ac: Long,
                           an: Long,
                           af: BigDecimal,
                           variant_class: String,
                           homozygotes: Long,
                           heterozygotes: Long,
                           study_id: String,
                           release_id: String)

  def variantOutput(chromosome: String = "2",
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
                    studyId: String = "SD_123456",
                    releaseId: String = "RE_ABCDEF"): VariantOutput =
    VariantOutput(
      chromosome, start, end, reference, alternate, hgvsg, name, ac, an, af, variant_class, homozygotes, heterozygotes, studyId, releaseId)

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

  case class ConsequenceOutput(chromosome: String,
                               start: Long,
                               end: Long,
                               reference: String,
                               alternate: String,
                               symbol: String,
                               impact: String,
                               ensembl_gene_id: String,
                               ensembl_transcript_id: Option[String],
                               ensembl_regulatory_id: Option[String],
                               feature_type: String,
                               consequence: String,
                               biotype: Option[String],
                               name: Option[String],
                               variant_class: String,
                               strand: Int,
                               hgvsg: Option[String],
                               hgvsc: Option[String],
                               hgvsp: Option[String],
                               exon: Option[Exon],
                               intron: Option[Intron],
                               cdna_position: Option[Int],
                               cds_position: Option[Int],
                               amino_acids: Option[RefAlt],
                               codons: Option[RefAlt],
                               protein_position: Option[Int],
                               study_id: String,
                               release_id: String
                              )

  def consequenceOutput(
                         chromosome: String = "2",
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
                         release_id: String = "RE_ABCDEF"): ConsequenceOutput =
    ConsequenceOutput(
      chromosome,
      start,
      end,
      reference,
      alternate,
      symbol,
      impact,
      ensembl_gene_id,
      ensembl_transcript_id,
      ensembl_regulatory_id,
      feature_type,
      consequence,
      biotype,
      name,
      variant_class,
      strand,
      hgvsg,
      hgvsc,
      hgvsp,
      exon,
      intron,
      cdna_position,
      cds_position,
      amino_acids,
      codons,
      protein_position,
      study_id,
      release_id
    )


  case class ConsequenceInput(
                               Allele: String,
                               Consequence: Seq[String],
                               IMPACT: String,
                               SYMBOL: String,
                               Gene: String,
                               Feature_type: String,
                               Feature: String,
                               STRAND: Int,
                               BIOTYPE: String,
                               VARIANT_CLASS: String,
                               HGVSg: String,
                               HGVSc: String,
                               HGVSp: String,
                               cDNA_position: Option[Int],
                               CDS_position: Option[Int],
                               Amino_acids: Option[RefAlt],
                               Protein_position: Option[Int],
                               Codons: Option[RefAlt],
                               EXON: Option[Exon],
                               INTRON: Option[Intron]
                             )

  def consequence(Allele: String = "A",
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
                  INTRON: Option[Intron] = None): ConsequenceInput =
    ConsequenceInput(
      Allele,
      Consequence,
      IMPACT,
      SYMBOL,
      Gene,
      Feature_type,
      Feature,
      STRAND,
      BIOTYPE,
      VARIANT_CLASS,
      HGVSg,
      HGVSc,
      HGVSp,
      cDNA_position,
      CDS_position,
      Amino_acids,
      Protein_position,
      Codons,
      EXON,
      INTRON
    )
}


//Array(ConsequenceOutput(2,165310405,165310405,G,A,SCN2A,MODERATE,ENSG00000136531,ENST00000283256.10,missense_variant,Some(protein_coding),Some(rs1057520413),SNV,1,Some(chr2:g.166166916G>A),Some(ENST00000488147.1:g.1000C>G),Some(ENSP00000283256.6:p.Val261Met),None,None,None,None,None,None,None,SD_123456,RE_ABCDEF), ConsequenceOutput(2,165310405,165310405,G,A,SCN2A,MODERATE,ENSG00000136531,ENST00000636135.1,missense_variant,Some(protein_coding),Some(rs1057520413),SNV,1,Some(chr2:g.166166916G>A),Some(ENST00000488147.1:g.1000C>G),Some(ENSP00000283256.6:p.Val261Met),None,None,None,None,None,None,None,SD_123456,RE_ABCDEF), ConsequenceOutput(2,165310405,165310405,G,A,SCN2A,MODERATE,ENSG00000136531,ENST00000636135.1,NMD_transcript_variant,Some(protein_coding),Some(rs1057520413),SNV,1,Some(chr2:g.166166916G>A),Some(ENST00000488147.1:g.1000C>G),Some(ENSP00000283256.6:p.Val261Met),None,None,None,None,None,None,None,SD_123456,RE_ABCDEF))
// List(ConsequenceOutput(2,165310405,165310405,G,A,SCN2A,MODERATE,ENSG00000136531,ENST00000283256.10,missense_variant,Some(protein_coding),Some(rs1057520413),SNV,1,Some(chr2:g.166166916G>A),Some(ENST00000283256.10:c.781G>A),Some(ENSP00000283256.6:p.Val261Met),Some(Exon(7,27)),None,Some(937),Some(781),Some(RefAlt(V,M)),Some(Codons(GTG,ATG)),Some(261),SD_123456,RE_ABCDEF), ConsequenceOutput(2,165310405,165310405,G,A,SCN2A,MODERATE,ENSG00000136531,ENST00000636135.1,missense_variant,Some(protein_coding),Some(rs1057520413),SNV,1,Some(chr2:g.166166916G>A),Some(ENST00000283256.10:c.781G>A),Some(ENSP00000283256.6:p.Val261Met),Some(Exon(7,27)),None,Some(937),Some(781),Some(RefAlt(V,M)),Some(Codons(GTG,ATG)),Some(261),SD_123456,RE_ABCDEF), ConsequenceOutput(2,165310405,165310405,G,A,SCN2A,MODERATE,ENSG00000136531,ENST00000636135.1,NMD_transcript_variant,Some(protein_coding),Some(rs1057520413),SNV,1,Some(chr2:g.166166916G>A),Some(ENST00000283256.10:c.781G>A),Some(ENSP00000283256.6:p.Val261Met),Some(Exon(7,27)),None,Some(937),Some(781),Some(RefAlt(V,M)),Some(Codons(GTG,ATG)),Some(261),SD_123456,RE_ABCDEF))

