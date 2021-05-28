package org.kidsfirstdrc.dwh.testutils.external

case class GnomadV311Output(
    chromosome: String = "1",
    start: Long = 1000,
    end: Long = 1010,
    reference: String = "A",
    alternate: String = "C",
    qual: Double = 0.5,
    name: String = "BRAF",
    ac: Long = 1,
    ac_raw: Long = 2,
    af: BigDecimal = BigDecimal(3.3),
    af_raw: BigDecimal = BigDecimal(4.4),
    an: Long = 5,
    an_raw: Long = 6,
    nhomalt: Long = 7,
    nhomalt_raw: Long = 8
)
