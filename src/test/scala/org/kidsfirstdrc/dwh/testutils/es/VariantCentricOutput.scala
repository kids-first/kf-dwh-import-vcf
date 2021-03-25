package org.kidsfirstdrc.dwh.testutils.es

import org.kidsfirstdrc.dwh.testutils.external._
import org.kidsfirstdrc.dwh.testutils.join.{Freq, OneThousandGenomesFreq}
import org.kidsfirstdrc.dwh.testutils.vcf.{Exon, Intron, RefAlt}

object VariantCentricOutput {

  case class GnomadFreqOutput(ac: Long = 10,
                              an: Long = 20,
                              af: BigDecimal = 0.5,
                              homozygotes: Long = 10)

  case class StudyFrequency(hmb: Freq, gru: Freq)

  case class Study(study_id: String,
                   acls: List[String],
                   external_study_ids: List[String],
                   frequencies: StudyFrequency,
                   participant_number: Long)

  case class InternalFrequencies(combined: Freq = Freq(34, 14, 0.411764705882352900, 14, 8),
                                 hmb: Freq = Freq(27, 12, 0.444444444400000000, 9, 7),
                                 gru: Freq = Freq(7, 2, 0.285714285700000000, 5, 1))

  case class Frequencies(one_thousand_genomes: OneThousandGenomesFreq = OneThousandGenomesFreq(),
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

  case class Consequence(vep_impact: String = "MODERATE", // index true
                         symbol: String, // index false
                         ensembl_transcript_id: Option[String] = Some("ENST00000283256.10"), //index false
                         ensembl_regulatory_id: Option[String] = None, //index false
                         hgvsc: Option[String] = Some("ENST00000283256.10:c.781G>A"),
                         hgvsp: Option[String] = Some("ENSP00000283256.6:p.Val261Met"),
                         feature_type: String = "Transcript",
                         consequences: Seq[String] = Seq("missense_variant"), // index true
                         biotype: Option[String] = Some("protein_coding"), // index true
                         variant_class: String = "SNV", // index true
                         strand: Int = 1, // index true
                         exon: Option[Exon] = Some(Exon(Some(7), Some(27))),
                         intron: Option[Intron] = None,
                         cdna_position: Option[Int] = Some(937),
                         cds_position: Option[Int] = Some(781),
                         amino_acids: Option[RefAlt] = Some(RefAlt("V", "M")),
                         codons: Option[RefAlt] = Some(RefAlt("GTG", "ATG")),
                         protein_position: Option[Int] = Some(261),
                         aa_change: Option[String] = Some("V261M"),
                         coding_dna_change: Option[String] = Some("781G>A"),
                         impact_score: Int = 3, // index true
                         canonical: Boolean = true, // index true
                         conservations: ScoreConservations, // index true
                         predictions: ScorePredictions) // index true

  case class GENES(`symbol`: Option[String] = Some("SCN2A"), //index true
                   `entrez_gene_id`: Option[Int] = Some(777), //index false
                   `omim_gene_id`: Option[String] = Some("601013"), //index false
                   `hgnc`: Option[String] = Some("HGNC:1392"), //index false
                   `ensembl_gene_id`: Option[String] = Some("ENSG00000189337"), //index false
                   `location`: Option[String] = Some("1q25.3"),//index false
                   `name`: Option[String] = Some("calcium voltage-gated channel subunit alpha1 E"),//index false
                   `alias`: Option[List[String]] = Some(List("BII", "CACH6", "CACNL1A6", "Cav2.3", "EIEE69", "gm139")),//index false
                   `orphanet`: List[ORPHANET] = List(ORPHANET()),
                   `hpo`: List[HPO] = List(HPO()),
                   `omim`: List[OMIM] = List(OMIM()),
                   `ddd`: List[DDD] = List(DDD()),
                   `cosmic`: List[COSMIC] = List(COSMIC()))

  //case class PARTICIPANT(`participant_id`: String = "PT_000003")

  case class Output(`hash`: String = "ba3d35feba14451058e6fc93eeba163c800a8e09",
                    `chromosome`: String = "2", //index true
                    `start`: Long = 165310406, //index true
                    `reference`: String = "G", //index false
                    `alternate`: String = "A", //index false
                    `locus`: String = "2-165310406-G-A", //index false
                    `studies`: List[Study] = List(), //index true
                    `participant_number`: Long = 22, //index true
                    `acls`: List[String] = List("SD_456.c1", "SD_123.c1", "SD_789.c99"), //index true
                    `external_study_ids`: List[String] = List("SD_456", "SD_123", "SD_789"), //index true
                    `frequencies`: Frequencies = Frequencies(), //index true
                    `clinvar`: CLINVAR = CLINVAR(), //index true
                    `rsnumber`: String = "rs1234567", //index false
                    `release_id`: String = "RE_ABCDEF", //index false
                    `consequences`: List[Consequence] = List(),
                    `hgvsg`: Option[String] = Some("chr2:g.166166916G>A"), //index false
                    `genes`: List[GENES] = List(GENES()),
                    `participant_ids`: List[String] = List("PT_000003"))

}
