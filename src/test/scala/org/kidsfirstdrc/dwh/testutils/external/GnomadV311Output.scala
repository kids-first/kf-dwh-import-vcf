package org.kidsfirstdrc.dwh.testutils.external

case class GnomadV311Output(
                             chromosome: String = "1",
                             start: Long = 1000,
                             end: Long = 1010,
                             reference: String = "A",
                             alternate: String = "C",
                             qual: Double = 0.5,
                             name: String = "BRAF",
                             ac: List[Long] = List(1),
                             ac_raw: List[Long] = List(2),
                             af: List[BigDecimal] = List(3),
                             af_raw: List[BigDecimal] = List(4),
                             an: Long = 5,
                             an_raw: Long = 6,
                             nhomalt_raw: Long = 7
                           )
