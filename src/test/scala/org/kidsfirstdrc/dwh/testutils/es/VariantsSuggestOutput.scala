/** Generated by [[bio.ferlab.datalake.spark3.ClassGenerator]]
  * on 2021-03-16T08:21:36.929385
  */
package org.kidsfirstdrc.dwh.testutils.es

case class VariantsSuggestOutput(`type`: String = "variant",
                                 `chromosome`: String = "2",
                                 `locus`: String = "2-165310406-G-A",
                                 `suggestion_id`: String = "ba3d35feba14451058e6fc93eeba163c800a8e09",
                                 `hgvsg`: String = "chr2:g.166166916G>A",
                                 `symbol`: String = "SCN2A",
                                 `rsnumber`: String = "rs1313905795",
                                 `suggest`: List[SUGGEST] =
                                 List(SUGGEST(), SUGGEST(List("SCN2A", "SCN2A.2", "ENSG00000136531", "ENST00000486878"), 2)))

case class SUGGEST(`input`: List[String] = List("SCN2A V261E", "SCN2A.2 V261M", "chr2:g.166166916G>A", "2-165310406-G-A", "rs1313905795", "RCV000436956"),
                   `weight`: Long = 4)