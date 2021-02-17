package org.kidsfirstdrc.dwh.testutils.external

case class OneKGenomesInput(contigName: String = "chr1",
                            start: Long = 999,
                            end: Long = 1009,
                            names: List[String] = List("BRAF"),
                            referenceAllele: String = "A",
                            alternateAlleles: List[String] = List("C"),
                            INFO_AC: List[Long] = List(0),
                            INFO_AF: List[BigDecimal] = List(0),
                            INFO_AN: Long = 0,
                            INFO_AFR_AF: List[BigDecimal] = List(0),
                            INFO_EUR_AF: List[BigDecimal] = List(0),
                            INFO_SAS_AF: List[BigDecimal] = List(0),
                            INFO_AMR_AF: List[BigDecimal] = List(0),
                            INFO_EAS_AF: List[BigDecimal] = List(0),
                            INFO_DP: Long = 0)
