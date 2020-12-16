package org.kidsfirstdrc.dwh.testutils.external

object OneKGenomes {
  case class OneKGenomesOutput(chromosome: String = "1",
                               start: Long = 1000,
                               end: Long = 1010,
                               reference: String = "A",
                               alternate: String = "C",
                               ac: Long = 0,
                               an: Long = 0,
                               af: BigDecimal = 0,
                               afr_af: BigDecimal = 0,
                               eur_af: BigDecimal = 0,
                               sas_af: BigDecimal = 0,
                               amr_af: BigDecimal = 0,
                               eas_af: BigDecimal = 0,
                               dp: Long)
}

