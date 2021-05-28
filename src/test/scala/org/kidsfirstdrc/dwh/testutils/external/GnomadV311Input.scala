package org.kidsfirstdrc.dwh.testutils.external

case class GnomadV311Input(
    contigName: String = "chr1",
    start: Long = 999,
    end: Long = 1009,
    referenceAllele: String = "A",
    alternateAlleles: List[String] = List("C"),
    qual: Double = 0.5,
    names: List[String] = List("BRAF"),
    INFO_AC: List[Long] = List(1),
    INFO_AC_raw: List[Long] = List(2),
    INFO_AF: List[BigDecimal] = List(BigDecimal(3.3)),
    INFO_AF_raw: List[BigDecimal] = List(BigDecimal(4.4)),
    INFO_AN: Long = 5,
    INFO_AN_raw: Long = 6,
    INFO_nhomalt: List[Long] = List(7),
    INFO_nhomalt_raw: List[Long] = List(8)
)
