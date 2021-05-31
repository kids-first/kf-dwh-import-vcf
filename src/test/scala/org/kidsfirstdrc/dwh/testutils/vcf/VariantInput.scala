package org.kidsfirstdrc.dwh.testutils.vcf

case class VariantInput(
    chromosome: String = "2",
    start: Long = 165310406,
    end: Long = 165310406,
    reference: String = "G",
    alternate: String = "A",
    hgvsg: String = "chr2:g.166166916G>A",
    name: Option[String] = Some("rs1057520413"),
    variant_class: String = "SNV",
    zygosity: String = "HOM",
    has_alt: Int = 1,
    is_hmb: Boolean = true,
    is_gru: Boolean = false,
    study_id: String = "SD_123456",
    release_id: String = "RE_ABCDEF",
    dbgap_consent_code: String = "SD_123456.c1"
)
