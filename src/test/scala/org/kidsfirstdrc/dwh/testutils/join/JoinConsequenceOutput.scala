package org.kidsfirstdrc.dwh.testutils.join

import org.kidsfirstdrc.dwh.testutils.vcf.{Exon, Intron, RefAlt}

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
                                 study_ids: Set[String] = Set("SD_123456"),
                                 release_id: String = "RE_ABCDEF",
                                 //sift_score: Option[Double] = Some(0.99),
                                 aa_change: Option[String] = Some("V261M"),
                                 coding_dna_change: Option[String] = Some("781G>A"),
                                 canonical: Boolean = true,
                                 //Scores from DBNSFP
                                 BayesDel_addAF_pred: Option[String] = None,
                                 BayesDel_addAF_rankscore: Option[String] = None,
                                 BayesDel_addAF_score: Option[String] = None,
                                 BayesDel_noAF_pred: Option[String] = None,
                                 BayesDel_noAF_rankscore: Option[String] = None,
                                 BayesDel_noAF_score: Option[String] = None,
                                 CADD_phred: Option[String] = None,
                                 CADD_raw: Option[String] = None,
                                 CADD_raw_rankscore: Option[String] = Some("CADD_raw_rankscore"),
                                 ClinPred_pred: Option[String] = None,
                                 ClinPred_rankscore: Option[String] = None,
                                 ClinPred_score: Option[String] = None,
                                 DANN_rankscore: Option[String] = Some("DANN_rankscore"),
                                 DANN_score: Option[String] = None,
                                 DEOGEN2_pred: Option[String] = None,
                                 DEOGEN2_rankscore: Option[String] = None,
                                 DEOGEN2_score: Option[String] = None,
                                 `Eigen-PC-phred_coding`: Option[String] = None,
                                 `Eigen-PC-raw_coding`: Option[String] = None,
                                 `Eigen-PC-raw_coding_rankscore`: Option[String] = None,
                                 `Eigen-phred_coding`: Option[String] = None,
                                 `Eigen-raw_coding`: Option[String] = None,
                                 `Eigen-raw_coding_rankscore`: Option[String] = None,
                                 FATHMM_converted_rankscore: Option[String] = Some("FATHMM_rankscore"),
                                 FATHMM_pred: Option[String] = Some("FATHMM_pred"),
                                 FATHMM_score: Option[String] = None,
                                 `GERP++_NR`: Option[String] = None,
                                 `GERP++_RS`: Option[String] = None,
                                 `GERP++_RS_rankscore`: Option[String] = None,
                                 GM12878_confidence_value: Option[String] = None,
                                 GM12878_fitCons_rankscore: Option[String] = None,
                                 GM12878_fitCons_score: Option[String] = None,
                                 GTEx_V8_gene: List[String] = null,
                                 GTEx_V8_tissue: List[String] = null,
                                 GenoCanyon_rankscore: Option[String] = None,
                                 GenoCanyon_score: Option[String] = None,
                                 `H1-hESC_confidence_value`: Option[String] = None,
                                 `H1-hESC_fitCons_rankscore`: Option[String] = None,
                                 `H1-hESC_fitCons_score`: Option[String] = None,
                                 HUVEC_confidence_value: Option[String] = None,
                                 HUVEC_fitCons_rankscore: Option[String] = None,
                                 HUVEC_fitCons_score: Option[String] = None,
                                 Interpro_domain: Option[String] = None,
                                 LINSIGHT: Option[String] = None,
                                 LINSIGHT_rankscore: Option[String] = None,
                                 `LIST-S2_pred`: Option[String] = None,
                                 `LIST-S2_rankscore`: Option[String] = None,
                                 `LIST-S2_score`: Option[String] = None,
                                 LRT_converted_rankscore: Option[Double] = Some(0.4),
                                 LRT_pred: Option[String] = Some("LRT_pred"),
                                 `M-CAP_pred`: Option[String] = None,
                                 `M-CAP_rankscore`: Option[Double] = None,
                                 `M-CAP_score`: Option[Double] = None,
                                 MPC_rankscore: Option[Double] = None,
                                 MPC_score: Option[Double] = None,
                                 MVP_rankscore: Option[Double] = None,
                                 MVP_score: Option[Double] = None,
                                 MetaLR_pred: Option[String] = None,
                                 MetaLR_rankscore: Option[Double] = None,
                                 MetaLR_score: Option[Double] = None,
                                 MetaSVM_pred: Option[String] = None,
                                 MetaSVM_rankscore: Option[Double] = None,
                                 MetaSVM_score: Option[Double] = None,
                                 MutPred_rankscore: Option[Double] = None,
                                 MutPred_score: Option[Double] = None,
                                 MutationAssessor_pred: Option[String] = None,
                                 MutationAssessor_rankscore: Option[Double] = None,
                                 MutationAssessor_score: Option[Double] = None,
                                 MutationTaster_converted_rankscore: Option[Double] = None,
                                 PROVEAN_converted_rankscore: Option[Double] = None,
                                 PROVEAN_pred: Option[String] = None,
                                 PROVEAN_score: Option[Double] = None,
                                 Polyphen2_HDIV_pred: Option[String] = None,
                                 Polyphen2_HDIV_rankscore: Option[Double] = None,
                                 Polyphen2_HDIV_score: Option[Double] = None,
                                 Polyphen2_HVAR_pred: Option[String] = Some("HVAR_pred"),
                                 Polyphen2_HVAR_rankscore: Option[Double] = Some(0.2),
                                 Polyphen2_HVAR_score: Option[Double] = None,
                                 PrimateAI_pred: Option[String] = None,
                                 PrimateAI_rankscore: Option[Double] = None,
                                 PrimateAI_score: Option[Double] = None,
                                 REVEL_rankscore: Option[Double] = Some(0.3),
                                 SIFT_converted_rankscore: Option[Double] = Some(0.1),
                                 SIFT_pred: Option[String] = Some("SIFT_pred"),
                                 SIFT_score: Option[Double] = None,
                                 VEST4_rankscore: Option[Double] = None,
                                 VEST4_score: Option[Double] = None,
                                 bStatistic: Option[Double] = None,
                                 bStatistic_converted_rankscore: Option[Double] = None,
                                 `fathmm-MKL_coding_group`: Option[String] = None,
                                 `fathmm-MKL_coding_pred`: Option[String] = None,
                                 `fathmm-MKL_coding_rankscore`: Option[Double] = None,
                                 `fathmm-MKL_coding_score`: Option[Double] = None,
                                 `fathmm-XF_coding_pred`: Option[String] = None,
                                 `fathmm-XF_coding_rankscore`: Option[Double] = None,
                                 `fathmm-XF_coding_score`: Option[Double] = None,
                                 integrated_confidence_value: Option[Double] = None,
                                 integrated_fitCons_rankscore: Option[Double] = None,
                                 integrated_fitCons_score: Option[Double] = None,
                                 phastCons100way_vertebrate: Option[Double] = None,
                                 phastCons100way_vertebrate_rankscore: Option[Double] = None,
                                 phastCons17way_primate: Option[Double] = None,
                                 phastCons17way_primate_rankscore: Option[Double] = None,
                                 phastCons30way_mammalian_rankscore: Option[Double] = None,
                                 phastcons30way_mammalian: Option[Double] = None,
                                 phyloP100way_vertebrate: Option[Double] = None,
                                 phyloP100way_vertebrate_rankscore: Option[Double] = None,
                                 phyloP17way_primate: Option[Double] = None,
                                 phyloP17way_primate_rankscore: Option[Double] = Some(0.5),
                                 phyloP30way_mammalian: Option[Double] = None,
                                 phyloP30way_mammalian_rankscore: Option[Double] = None)
