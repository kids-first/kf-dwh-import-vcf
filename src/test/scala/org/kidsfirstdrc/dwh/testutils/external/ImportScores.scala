package org.kidsfirstdrc.dwh.testutils.external

object ImportScores {

  case class Output(
      `chromosome`: String = "2",
      `start`: Long = 165310406,
      `reference`: String = "G",
      `alternate`: String = "A",
      `aaref`: String = "V",
      `symbol`: String = "SCN2A",
      `ensembl_gene_id`: String = "ENSG00000136531",
      `ensembl_protein_id`: String = "ENSP00000487466",
      `VEP_canonical`: String = ".",
      `ensembl_transcript_id`: String = "ENST00000486878",
      `cds_strand`: String = null,
      `SIFT_score`: Option[Double] = None,
      `SIFT_pred`: Option[String] = None,
      `SIFT_converted_rankscore`: Option[Double] = Some(0.91255),
      `Polyphen2_HDIV_score`: Option[Double] = None,
      `Polyphen2_HDIV_pred`: Option[String] = None,
      `Polyphen2_HDIV_rankscore`: Option[Double] = Some(0.90584),
      `Polyphen2_HVAR_score`: Option[Double] = None,
      `Polyphen2_HVAR_pred`: Option[String] = None,
      `Polyphen2_HVAR_rankscore`: Option[Double] = Some(0.97372),
      `FATHMM_score`: Option[Double] = None,
      `FATHMM_pred`: Option[String] = None,
      `FATHMM_converted_rankscore`: Option[Double] = Some(0.98611),
      `CADD_raw`: Option[Double] = Some(3.941617),
      `CADD_raw_rankscore`: Option[Double] = Some(0.76643),
      `CADD_phred`: Option[Double] = Some(26.6),
      `DANN_score`: Option[Double] = Some(0.9988206585102238),
      `DANN_rankscore`: Option[Double] = Some(0.95813),
      `REVEL_rankscore`: Option[Double] = Some(0.98972),
      `LRT_converted_rankscore`: Option[Double] = Some(0.62929),
      `LRT_pred`: String = "D",
      `phyloP100way_vertebrate`: Option[Double] = Some(10.003),
      `phyloP100way_vertebrate_rankscore`: Option[Double] = Some(0.99689),
      `phyloP30way_mammalian`: Option[Double] = Some(1.176),
      `phyloP30way_mammalian_rankscore`: Option[Double] = Some(0.78918),
      `phyloP17way_primate`: Option[Double] = Some(0.676),
      `phyloP17way_primate_rankscore`: Option[Double] = Some(0.7674),
      `phastCons100way_vertebrate`: Option[Double] = Some(1.0),
      `phastCons100way_vertebrate_rankscore`: Option[Double] = Some(0.71638),
      `phastcons30way_mammalian`: Option[Double] = Some(0.959),
      `phastCons30way_mammalian_rankscore`: Option[Double] = Some(0.43061),
      `phastCons17way_primate`: Option[Double] = Some(0.993),
      `phastCons17way_primate_rankscore`: Option[Double] = Some(0.69303),
      `GERP++_NR`: Option[Double] = Some(5.27),
      `GERP++_RS`: Option[Double] = Some(5.27),
      `GERP++_RS_rankscore`: Option[Double] = Some(0.73797),
      `MutPred_rankscore`: Option[Double] = Some(0.9913),
      `MutPred_score`: Option[Double] = Some(0.938),
      `MutationAssessor_pred`: Option[String] = None,
      `MutationAssessor_score`: Option[Double] = None,
      `MutationAssessor_rankscore`: Option[Double] = Some(0.72352),
      `MutationTaster_converted_rankscore`: Option[Double] = Some(0.81001),
      `PROVEAN_pred`: Option[String] = None,
      `PROVEAN_score`: Option[Double] = None,
      `PROVEAN_converted_rankscore`: Option[Double] = Some(0.61435),
      `VEST4_score`: Option[Double] = None,
      `VEST4_rankscore`: Option[Double] = Some(0.9455),
      `MetaSVM_pred`: String = "D",
      `MetaSVM_rankscore`: Option[Double] = Some(0.99842),
      `MetaSVM_score`: Option[Double] = Some(1.1073),
      `MetaLR_pred`: String = "D",
      `MetaLR_rankscore`: Option[Double] = Some(0.99027),
      `MetaLR_score`: Option[Double] = Some(0.9692),
      `M-CAP_pred`: String = "D",
      `M-CAP_score`: Option[Double] = Some(0.874763),
      `M-CAP_rankscore`: Option[Double] = Some(0.99043),
      `MPC_score`: Option[Double] = None,
      `MPC_rankscore`: Option[Double] = None,
      `MVP_score`: Option[Double] = Some(0.971877060599),
      `MVP_rankscore`: Option[Double] = Some(0.97157),
      `PrimateAI_pred`: String = "D",
      `PrimateAI_rankscore`: Option[Double] = Some(0.98922),
      `PrimateAI_score`: Option[Double] = Some(0.928860723972),
      `DEOGEN2_pred`: String = "T",
      `DEOGEN2_score`: Option[Double] = Some(0.336344),
      `DEOGEN2_rankscore`: Option[Double] = Some(0.992),
      `BayesDel_addAF_pred`: String = "D",
      `BayesDel_addAF_rankscore`: Option[Double] = Some(0.95512),
      `BayesDel_addAF_score`: Option[Double] = Some(0.543005),
      `BayesDel_noAF_pred`: String = "D",
      `BayesDel_noAF_rankscore`: Option[Double] = Some(0.95445),
      `BayesDel_noAF_score`: Option[Double] = Some(0.542213),
      `ClinPred_pred`: String = "D",
      `ClinPred_rankscore`: Option[Double] = Some(0.91964),
      `ClinPred_score`: Option[Double] = Some(0.997604310512543),
      `LIST-S2_pred`: String = "D",
      `LIST-S2_score`: Option[Double] = Some(0.962204),
      `LIST-S2_rankscore`: Option[Double] = Some(0.92224),
      `fathmm-MKL_coding_pred`: String = "D",
      `fathmm-MKL_coding_rankscore`: Option[Double] = Some(0.89257),
      `fathmm-MKL_coding_score`: Option[Double] = Some(0.98964),
      `fathmm-MKL_coding_group`: String = "AEFI",
      `fathmm-XF_coding_pred`: String = "D",
      `fathmm-XF_coding_rankscore`: Option[Double] = Some(0.88927),
      `fathmm-XF_coding_score`: Option[Double] = Some(0.919156),
      `Eigen-PC-phred_coding`: Option[Double] = Some(12.83396),
      `Eigen-PC-raw_coding`: Option[Double] = Some(0.87780253437664),
      `Eigen-PC-raw_coding_rankscore`: Option[Double] = Some(0.94531),
      `Eigen-phred_coding`: Option[Double] = Some(11.24159),
      `Eigen-raw_coding`: Option[Double] = Some(0.907355178300014),
      `Eigen-raw_coding_rankscore`: Option[Double] = Some(0.92114),
      `GenoCanyon_rankscore`: Option[Double] = Some(0.74766),
      `GenoCanyon_score`: Option[Double] = Some(0.999999999920192),
      `integrated_confidence_value`: Option[Double] = Some(0.0),
      `integrated_fitCons_rankscore`: Option[Double] = Some(0.25195),
      `integrated_fitCons_score`: Option[Double] = Some(0.553676),
      `GM12878_confidence_value`: Option[Double] = Some(0.0),
      `GM12878_fitCons_rankscore`: Option[Double] = Some(0.26702),
      `GM12878_fitCons_score`: Option[Double] = Some(0.573888),
      `H1-hESC_confidence_value`: Option[Double] = Some(0.0),
      `H1-hESC_fitCons_rankscore`: Option[Double] = Some(0.43123),
      `H1-hESC_fitCons_score`: Option[Double] = Some(0.618467),
      `HUVEC_confidence_value`: Option[Double] = Some(0.0),
      `HUVEC_fitCons_rankscore`: Option[Double] = Some(0.26826),
      `HUVEC_fitCons_score`: Option[Double] = Some(0.564101),
      `LINSIGHT`: Option[Double] = None,
      `LINSIGHT_rankscore`: Option[Double] = None,
      `bStatistic`: Option[Double] = Some(761.0),
      `bStatistic_converted_rankscore`: Option[Double] = Some(0.50382),
      `Interpro_domain`: String = "Ion transport domain",
      `GTEx_V8_gene`: Option[List[String]] = None,
      `GTEx_V8_tissue`: Option[List[String]] = None
  )

}
