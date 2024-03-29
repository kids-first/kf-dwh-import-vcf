package org.kidsfirstdrc.dwh.testutils.vcf

case class OccurrenceOutput(
    `chromosome`: String = "2",
    `start`: Long = 165310407,
    `end`: Long = 165310407,
    `reference`: String = "G",
    `alternate`: String = "A",
    `name`: Option[String] = None,
    `biospecimen_id`: String = "BS_HIJKKL2",
    `participant_id`: String = "PT_000002",
    `family_id`: String = "FA_000001",
    `study_id`: String = "SD_123456",
    `release_id`: String = "RE_ABCDEF",
    `file_name`: String = "file_1",
    `dbgap_consent_code`: String = "phs001738.c1",
    `is_gru`: Boolean = true,
    `is_hmb`: Boolean = false,
    `ad`: List[Int] = List(34, 0),
    `dp`: Int = 34,
    `gq`: Int = 99,
    `calls`: List[Int] = List(0, 0),
    `has_alt`: Int = 0,
    `is_multi_allelic`: Boolean = false,
    `old_multi_allelic`: Option[String] = None,
    `quality`: Double = 39.48,
    `filter`: String = "VQSRTrancheINDEL99.90to99.95",
    `info_ac`: Int = 1,
    `info_an`: Int = 6,
    `info_af`: Double = 0.167,
    `info_culprit`: String = "DP",
    `info_sor`: Double = 0.732,
    `info_read_pos_rank_sum`: Option[Double] = Some(2.69),
    `info_inbreeding_coeff`: Option[Double] = None,
    `info_pg`: List[Int] = List(0, 0, 0),
    `info_fs`: Double = 0.0,
    `info_dp`: Int = 334,
    `info_ds`: Option[Boolean] = None,
    `info_info_negative_train_site`: Option[Boolean] = Some(true),
    `info_positive_train_site`: Option[Boolean] = None,
    `info_vqslod`: Double = -7.719,
    `info_clipping_rank_sum`: Option[Double] = Some(0.228),
    `info_raw_mq`: Option[Double] = Some(46212.0),
    `info_base_qrank_sum`: Option[Double] = Some(-0.959),
    `info_mleaf`: Double = 0.167,
    `info_mleac`: Int = 1,
    `info_mq`: Double = 36.34,
    `info_qd`: Double = 1.72,
    `info_db`: Option[Boolean] = None,
    `info_m_qrank_sum`: Option[Double] = Some(-0.502),
    `info_excess_het`: Double = 3.6798,
    `info_haplotype_score`: Option[Double] = None,
    `is_lo_conf_denovo`: Option[Boolean] = None,
    `is_hi_conf_denovo`: Option[Boolean] = None,
    `hgvsg`: String = "chr4:g.73979437G>T",
    `variant_class`: String = "SNV",
    `is_proband`: Boolean = true,
    `affected_status`: Boolean = true,
    `mother_id`: Option[String] = None,
    `father_id`: Option[String] = None,
    `mother_calls`: Option[List[Int]] = None,
    `father_calls`: Option[List[Int]] = None,
    `mother_affected_status`: Option[Boolean] = None,
    `father_affected_status`: Option[Boolean] = None,
    `parental_origin`: Option[String] = None,
    `transmission`: Option[String] = None,
    `zygosity`: String = "WT",
    `mother_zygosity`: Option[String] = None,
    `father_zygosity`: Option[String] = None,
    `is_normalized`: Boolean = true,
    `gender`: String = "Male"
)
