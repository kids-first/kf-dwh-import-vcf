package org.kidsfirstdrc.dwh.testutils.external

case class TopmedBravoOutput(
    chromosome: String = "2",
    start: Long = 69359261,
    end: Long = 69359262,
    name: String = "257668",
    reference: String = "T",
    alternate: String = "A",
    ac: Int = 5,
    af: Double = 0.5,
    an: Int = 10,
    homozygotes: Int = 5,
    heterozygotes: Int = 0,
    qual: Double = 0.8,
    filters: Array[String] = Array(),
    qual_filter: String = ""
)
