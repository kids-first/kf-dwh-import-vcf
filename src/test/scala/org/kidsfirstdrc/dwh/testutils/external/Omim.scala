package org.kidsfirstdrc.dwh.testutils.external

import org.kidsfirstdrc.dwh.external.omim.OmimPhenotype

object Omim {

  case class Input(_c0: String = "2",
                   _c1: Long = 165310406,
                   _c2: Long = 165310406,
                   _c3: String = "N2",
                   _c4: String = "N2TP",
                   _c5: String = "23234234",
                   _c6: String = "A, A",
                   _c7: String = "name",
                   _c8: String = "B",
                   _c9: String = "12345",
                   _c10: String = "ENSG00000136531",
                   _c11: String = "Some comments",
                   _c12: String = "[Bone mineral density QTL 3], 606928 (2)")

  case class Output(chromosome: String = "2",
                    start: Long = 165310406,
                    end: Long = 165310406,
                    cypto_location: String = "N2",
                    computed_cypto_location: String = "N2TP",
                    omim_gene_id: String = "23234234",
                    symbols : List[String] = List("A", "A"),
                    name: String = "name",
                    approved_symbol: String = "B",
                    entrez_gene_id: String = "12345",
                    ensembl_gene_id: String = "ENSG00000136531",
                    comments: String = "Some comments",
                    phenotype: Option[OmimPhenotype] = Some(OmimPhenotype("[Bone mineral density QTL 3]", "606928", None)))

}
