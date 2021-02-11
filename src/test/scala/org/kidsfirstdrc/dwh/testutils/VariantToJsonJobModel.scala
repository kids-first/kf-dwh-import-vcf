package org.kidsfirstdrc.dwh.testutils

import org.kidsfirstdrc.dwh.testutils.Model.{Exon, Freq, Intron, RefAlt}

object VariantToJsonJobModel {

  case class StudyFrequency(hmb: Freq, gru: Freq)

  case class Study(study_id: String,
                   acls: List[String],
                   external_study_ids: List[String],
                   frequencies: StudyFrequency,
                   hmb_participant_number: Long,
                   gru_participant_number: Long)

  case class InternalFrequencies(hmb: Freq = Freq(27,12,0.444444444400000000, Some(9), Some(7)),
                                 gru: Freq = Freq(7,2,0.285714285700000000, Some(5), Some(1)))

  case class Frequencies(/*ignored - tested separately  `1k_genomes`: Freq,*/
                         topmed: Freq = Freq(),
                         gnomad_genomes_2_1: Freq = Freq(heterozygotes = None),
                         gnomad_exomes_2_1: Freq = Freq(heterozygotes = None),
                         gnomad_genomes_3_0: Freq = Freq(heterozygotes = None),
                         internal: InternalFrequencies = InternalFrequencies())

  case class Clinvar(name: String, clin_sig: String)

  case class ScoreConservations(phylo_p17way_primate_rankscore: Double)

  case class ScorePredictions(sift_converted_rank_score: Double,
                              sift_pred: String,
                              polyphen2_hvar_score: Double,
                              polyphen2_hvar_pred: String,
                              FATHMM_converted_rankscore: String,
                              fathmm_pred: String,
                              cadd_score: String,
                              dann_score: String,
                              revel_rankscore: Double,
                              lrt_converted_rankscore: Double,
                              lrt_pred: String)
  case class ConsequenceScore(conservations: ScoreConservations,
                              predictions: ScorePredictions)

  case class Consequence(symbol: String = "SCN2A",
                         impact: String = "MODERATE",
                         ensembl_gene_id: String = "ENSG00000136531",
                         ensembl_transcript_id: Option[String] = Some("ENST00000283256.10"),
                         ensembl_regulatory_id: Option[String] = None,
                         feature_type: String = "Transcript",
                         consequences: Seq[String] = Seq("missense_variant"),
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
                         aa_change: Option[String] = Some("V261M"),
                         coding_dna_change: Option[String] = Some("781G>A"),
                         canonical: Boolean = true,
                         scores: ConsequenceScore)

  case class Output(chromosome: String,
                    start: Long,
                    end: Long,
                    reference: String,
                    alternate: String,
                    studies: List[Study],
                    hmb_participant_number: Long,
                    gru_participant_number: Long,
                    frequencies: Frequencies,
                    clinvar: Clinvar,
                    dbsnp_id: String,
                    release_id: String,
                    consequences: List[Consequence])
}
