/**
 * Generated by [[org.kidsfirstdrc.dwh.testutils.ClassGenerator]]
 * on 2021-01-28T15:59:06.294
 */
package org.kidsfirstdrc.dwh.testutils.variant

import org.kidsfirstdrc.dwh.testutils.Model.Freq

case class Variant(chromosome: String = "2",
                   start: Long = 69359261,
                   end: Long = 69359262,
                   reference: String = "T",
                   alternate: String = "A",
                   name: String = "rs112682152",
                   hgvsg: String = "chr2:g.69359261T>A",
                   variant_class: String = "SNV",
                   hmb_ac: Long = 1,
                   hmb_an: Long = 2,
                   hmb_af: BigDecimal = 0.5,
                   hmb_homozygotes: Long = 1,
                   hmb_heterozygotes: Long = 1,
                   gru_ac: Long = 0,
                   gru_an: Long = 0,
                   gru_af: BigDecimal = 0,
                   gru_homozygotes: Long = 0,
                   gru_heterozygotes: Long = 0,
                   //1k_genomes: Option[Freq] = Some(Freq(20, 10, 0.5, Some(10), Some(10))),
                   onek_genomes: Option[Freq] = Some(Freq(20, 10, 0.5, 10, 10)),
                   topmed: Option[Freq] = Some(Freq(20, 10, 0.5, 10, 10)),
                   gnomad_genomes_2_1: Option[Freq] = Some(Freq(20, 10, 0.5, 10, 10)),
                   gnomad_exomes_2_1: Option[Freq] = Some(Freq(20, 10, 0.5, 10, 10)),
                   gnomad_genomes_3_0: Option[Freq] = Some(Freq(20, 10, 0.5, 10, 10)),
                   clinvar_id: Option[String] = Some("RCV000436956"),
                   clin_sig: List[String] = List("Pathogenic"),
                   dbsnp_id: Option[String] = Some("rs1234567"),
                   hmb_ac_by_study: Map[String, Long] = Map(),
                   hmb_an_by_study: Map[String, Long] = Map(),
                   hmb_af_by_study: Map[String, BigDecimal]  = Map(),
                   hmb_homozygotes_by_study: Map[String, Long]  = Map(),
                   hmb_heterozygotes_by_study: Map[String, Long]  = Map(),
                   gru_ac_by_study: Map[String, Long]  = Map(),
                   gru_an_by_study: Map[String, Long]  = Map(),
                   gru_af_by_study: Map[String, BigDecimal]  = Map(),
                   gru_homozygotes_by_study: Map[String, Long]  = Map(),
                   gru_heterozygotes_by_study: Map[String, Long]  = Map(),
                   studies: Set[String] = Set(),
                   consent_codes: Set[String] = Set(),
                   consent_codes_by_study: Map[String, Set[String]] = Map(),
                   release_id: String = "RE_ABCDEF")