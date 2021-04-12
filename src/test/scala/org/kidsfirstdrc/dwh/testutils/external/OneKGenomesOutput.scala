package org.kidsfirstdrc.dwh.testutils.external

case class OneKGenomesOutput(chromosome: String = "1",
                             start: Long = 1000,
                             end: Long = 1010,
                             name: String = "BRAF",
                             reference: String = "A",
                             alternate: String = "C",
                             ac: Long = 0,
                             af: Double = 0,
                             an: Long = 0,
                             afr_af: Double = 0,
                             eur_af: Double = 0,
                             sas_af: Double = 0,
                             amr_af: Double = 0,
                             eas_af: Double = 0,
                             dp: Long = 0)
