package org.kidsfirstdrc.dwh.testutils.vcf

import org.kidsfirstdrc.dwh.testutils.join.Freq

case class VariantOutput(chromosome: String = "2",
                         start: Long = 165310406,
                         end: Long = 165310406,
                         reference: String = "G",
                         alternate: String = "A",
                         hgvsg: String = "chr2:g.166166916G>A",
                         name: Option[String] = Some("rs1057520413"),
                         frequencies: VariantFrequency = VariantFrequency(),
                         variant_class: String = "SNV",
                         study_id: String = "SD_123456",
                         release_id: String = "RE_ABCDEF",
                         consent_codes: Set[String] = Set("SD_123456.c1"),
                         consent_codes_by_study: Map[String, Set[String]] = Map("SD_123456" -> Set("SD_123456.c1")))

case class VariantFrequency(upper_bound_kf: Freq = Freq(2, 2, 1, 1, 0),
                            lower_bound_kf: Freq = Freq(2, 2, 1, 1, 0))
