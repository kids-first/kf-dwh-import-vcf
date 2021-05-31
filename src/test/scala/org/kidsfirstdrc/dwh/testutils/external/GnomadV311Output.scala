package org.kidsfirstdrc.dwh.testutils.external

case class GnomadV311Output(
    chromosome: String = "2",
    start: Long = 165310406,
    end: Long = 165310407,
    reference: String = "G",
    alternate: String = "A",
    ac: Long = 10,
    ac_raw: Long = 11,
    an: Long = 20,
    an_raw: Long = 21,
    af: Double = 0.5,
    af_raw: Double = 0.6,
    nhomalt: Long = 10,
    nhomalt_raw: Long = 11,
    qual: Double = 0.5,
    name: String = "BRAF"
)
