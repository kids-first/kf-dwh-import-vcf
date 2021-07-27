package org.kidsfirstdrc.dwh.testutils.join

import org.kidsfirstdrc.dwh.testutils.vcf.VariantFrequency

case class JoinVariantOutput(
    chromosome: String = "2",
    start: Long = 165310406,
    end: Long = 165310406,
    reference: String = "G",
    alternate: String = "A",
    name: String = "rs1057520413",
    hgvsg: String = "chr2:g.166166916G>A",
    variant_class: String = "SNV",
    frequencies: VariantFrequency = VariantFrequency(Freq(2, 1, 0.5, 1, 1), Freq(2, 1, 0.5, 1, 1)),
    one_thousand_genomes: Option[OneThousandGenomesFreq] = Some(OneThousandGenomesFreq(20, 10, 0.5)),
    topmed: Option[Freq] = Some(Freq(20, 10, 0.5, 10, 10)),
    gnomad_genomes_2_1: Option[GnomadFreq] = Some(GnomadFreq(10, 20, 0.5, 10)),
    gnomad_exomes_2_1: Option[GnomadFreq] = Some(GnomadFreq(10, 20, 0.5, 10)),
    gnomad_genomes_3_0: Option[GnomadFreq] = Some(GnomadFreq(10, 20, 0.5, 10)),
    gnomad_genomes_3_1_1: Option[GnomadFreq] = Some(GnomadFreq(10, 20, 0.5, 10)),
    clinvar_id: Option[String] = Some("RCV000436956"),
    clin_sig: Option[String] = Some("Pathogenic"),
    dbsnp_id: Option[String] = Some("rs1234567"),
    upper_bound_kf_ac_by_study: Map[String, Long],
    upper_bound_kf_an_by_study: Map[String, Long],
    upper_bound_kf_af_by_study: Map[String, Double],
    upper_bound_kf_homozygotes_by_study: Map[String, Long],
    upper_bound_kf_heterozygotes_by_study: Map[String, Long],
    lower_bound_kf_ac_by_study: Map[String, Long],
    lower_bound_kf_an_by_study: Map[String, Long],
    lower_bound_kf_af_by_study: Map[String, Double],
    lower_bound_kf_homozygotes_by_study: Map[String, Long],
    lower_bound_kf_heterozygotes_by_study: Map[String, Long],
    studies: Set[String],
    consent_codes: Set[String],
    consent_codes_by_study: Map[String, Set[String]],
    transmissions: Map[String, Int],
    transmissions_by_study: Map[String, Map[String, Int]],
    release_id: String = "RE_ABCDEF"
)

case class OneThousandGenomesFreq(an: Long = 20, ac: Long = 10, af: Double = 0.5)

case class GnomadFreq(ac: Long = 10, an: Long = 20, af: Double = 0.5, hom: Long = 10)

case class Freq(
    an: Long = 20,
    ac: Long = 10,
    af: Double = 0.5,
    homozygotes: Long = 10,
    heterozygotes: Long = 10
)
