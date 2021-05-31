package org.kidsfirstdrc.dwh.testutils.external

case class GnomadV311Input(
    contigName: String = "chr2",
    start: Long = 165310405,
    end: Long = 165310406,
    referenceAllele: String = "G",
    alternateAlleles: List[String] = List("A"),
    qual: Double = 0.5,
    names: List[String] = List("BRAF"),
    INFO_AC: List[Long] = List(10),
    INFO_AC_raw: List[Long] = List(11),
    INFO_AF: List[Double] = List(0.5),
    INFO_AF_raw: List[Double] = List(0.6),
    INFO_AN: Long = 20,
    INFO_AN_raw: Long = 21,
    INFO_nhomalt: List[Long] = List(10),
    INFO_nhomalt_raw: List[Long] = List(11)
)
