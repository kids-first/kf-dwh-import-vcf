package org.kidsfirstdrc.dwh.testutils

object Model {

  case class AnnotationInput(contigName: String,
                             start: Long,
                             end: Long,
                             referenceAllele: String,
                             alternateAlleles: Seq[String],
                             INFO_AC: Seq[Long],
                             INFO_AN: Long,
                             names: Seq[String],
                             INFO_ANN: Seq[AnnInput],
                             genotypes: Seq[Genotype],
                             splitFromMultiAllelic: Boolean = false
                            )

  case class AnnotationOutput(chromosome: String,
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

  case class ConsequenceOutput(chromosome: String,
                               start: Long,
                               end: Long,
                               reference: String,
                               alternate: String,
                               symbol: String,
                               impact: String,
                               ensembl_gene_id: String,
                               consequence: String,
                               strand: Int,
                               hgvsg: String,
                               name: Option[String],
                               variant_class: String,
                               transcripts: Seq[String],
                               study_id: String,
                               release_id: String,
                               cds_position: Option[Int],
                               amino_acids: Option[AminoAcids],
                               protein_position: Option[Int]
                              )

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

  case class AminoAcids(reference: String, variant: String)

  case class AnnInput(Allele: String, Consequence: Seq[String], IMPACT: String, SYMBOL: String, Gene: String, Feature: String, STRAND: Int, VARIANT_CLASS: String, HGVSg: String, CDS_position: Option[Int], Amino_acids: Option[AminoAcids], Protein_position: Option[Int])

}

