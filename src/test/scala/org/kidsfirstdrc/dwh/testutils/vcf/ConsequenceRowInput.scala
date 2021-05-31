package org.kidsfirstdrc.dwh.testutils.vcf

case class ConsequenceRowInput(
    chromosome: String = "2",
    start: Long = 165310406,
    end: Long = 165310406,
    reference: String = "G",
    alternate: String = "A",
    name: String = "rs1057520413",
    annotations: Seq[ConsequenceInput] = Seq(ConsequenceInput()),
    splitFromMultiAllelic: Boolean = false
)
