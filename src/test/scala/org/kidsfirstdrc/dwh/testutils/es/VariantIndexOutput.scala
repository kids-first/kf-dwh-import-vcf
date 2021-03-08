package org.kidsfirstdrc.dwh.testutils.es

import org.kidsfirstdrc.dwh.testutils.Model._

object VariantIndexOutput {

  case class StudyFrequency(hmb: Freq, gru: Freq)

  case class Study(study_id: String,
                   acls: List[String],
                   external_study_ids: List[String],
                   frequencies: StudyFrequency,
                   participant_number: Long)

  case class InternalFrequencies(combined: Freq = Freq(34, 14, 0.411764705882352900, 14, 8),
                                 hmb: Freq = Freq(27, 12, 0.444444444400000000, 9, 7),
                                 gru: Freq = Freq(7, 2, 0.285714285700000000, 5, 1))

  case class Frequencies(/*ignored - tested separately  `1k_genomes`: Freq,*/
                         topmed: Freq = Freq(),
                         gnomad_genomes_2_1: GnomadFreqOutput = GnomadFreqOutput(),
                         gnomad_exomes_2_1: GnomadFreqOutput = GnomadFreqOutput(),
                         gnomad_genomes_3_0: GnomadFreqOutput = GnomadFreqOutput(),
                         internal: InternalFrequencies = InternalFrequencies())

  case class CLINVAR(`clinvar_id`: String = "257668",
                     `clin_sig`: List[String] = List("Benign"),
                     `conditions`: List[String] = List("Congenital myasthenic syndrome 12", "not specified", "not provided"),
                     `inheritance`: List[String] = List("germline"),
                     `interpretations`: List[String] = List("", "Benign"))

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

  case class Consequence(vep_impact: String = "MODERATE",
                         symbol: String,
                         ensembl_transcript_id: Option[String] = Some("ENST00000283256.10"),
                         ensembl_regulatory_id: Option[String] = None,
                         hgvsc: Option[String] = Some("ENST00000283256.10:c.781G>A"),
                         hgvsp: Option[String] = Some("ENSP00000283256.6:p.Val261Met"),
                         feature_type: String = "Transcript",
                         consequences: Seq[String] = Seq("missense_variant"),
                         biotype: Option[String] = Some("protein_coding"),
                         variant_class: String = "SNV",
                         strand: Int = 1,
                         exon: Option[Exon] = Some(Exon(7, 27)),
                         intron: Option[Intron] = None,
                         cdna_position: Option[Int] = Some(937),
                         cds_position: Option[Int] = Some(781),
                         amino_acids: Option[RefAlt] = Some(RefAlt("V", "M")),
                         codons: Option[RefAlt] = Some(RefAlt("GTG", "ATG")),
                         protein_position: Option[Int] = Some(261),
                         aa_change: Option[String] = Some("V261M"),
                         coding_dna_change: Option[String] = Some("781G>A"),
                         impact_score: Int = 3,
                         canonical: Boolean = true,
                         conservations: ScoreConservations,
                         predictions: ScorePredictions)


  case class Output(chromosome: String = "2",
                    start: Long = 165310406,
                    end: Long = 165310406,
                    reference: String = "G",
                    alternate: String = "A",
                    locus: String = "2-165310406-G-A",
                    studies: List[Study] = List(),
                    participant_number: Long = 22,
                    acls: List[String] = List("SD_456.c1", "SD_123.c1", "SD_789.c99"),
                    external_study_ids: List[String] = List("SD_456", "SD_123", "SD_789"),
                    frequencies: Frequencies = Frequencies(),
                    clinvar: CLINVAR = CLINVAR(),
                    rsnumber: String = "rs1234567",
                    release_id: String = "RE_ABCDEF",
                    consequences: List[Consequence] = List(),
                    //symbols: List[String] = List("SCN2A"),
                    orphanet_disorder_ids: List[Long] = List(17601),
                    panels: List[String] = List("Multiple epiphyseal dysplasia, Al-Gazali type"),
                    inheritances: List[String] = List("Autosomal recessive"),
                    hgvsg: Option[String] = Some("chr2:g.166166916G>A"),
                    disease_names: List[String] = List("OCULOAURICULAR SYNDROME"),
                    tumour_types_germlines: List[String] = List(),
                    omim_gene_ids: List[String] = List("23234234"),
                    entrez_gene_ids: List[String] = List("12345"),
                    ensembl_gene_ids: List[String] = List("ENSG00000189337"))

}
