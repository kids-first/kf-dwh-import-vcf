/**
 * Generated by [[bio.ferlab.datalake.core.ClassGenerator]]
 * on 2021-03-16T08:21:36.929385
 */
package org.kidsfirstdrc.dwh.testutils


case class SuggesterOutput(`type`: String = "variant",
                           `locus`: String = "2-165310406-G-A",
                           `suggestion_id`: String = "ba3d35feba14451058e6fc93eeba163c800a8e09",
                           `hgvsg`: String = "chr2:g.166166916G>A",
                           `suggest`: List[SUGGEST] = List(SUGGEST(), SUGGEST(List("SCN2A", "SCN2A.2"), 2)),
                           `consequences`: List[CONSEQUENCE] = List(CONSEQUENCE(), CONSEQUENCE("SCN2A.2", "V261M")))


case class SUGGEST(`input`: List[String] = List("SCN2A V261E", "SCN2A.2 V261M", "chr2:g.166166916G>A", "2-165310406-G-A"),
                   `weight`: Long = 4)

case class CONSEQUENCE(`symbol`: String = "SCN2A",
                       `aa_change`: String = "V261E")
