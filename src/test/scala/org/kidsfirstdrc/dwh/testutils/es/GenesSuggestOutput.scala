
package org.kidsfirstdrc.dwh.testutils.es

case class GenesSuggestOutput(`type`: String = "variant",
                              //`chromosome`: String = "2",
                              //`locus`: String = "2-165310406-G-A",
                              `suggestion_id`: String = "ba3d35feba14451058e6fc93eeba163c800a8e09",
                              //`hgvsg`: String = "chr2:g.166166916G>A",
                              `symbol`: String = "SCN2A",
                              `ensembl_gene_id`: String = "ENSG00000136531",
                              //`rsnumber`: String = "rs1313905795",
                              `suggest`: List[SUGGEST] =
                              List(SUGGEST(), SUGGEST(List("SCN2A", "SCN2A.2", "ENSG00000136531", "ENST00000486878"), 2)))

