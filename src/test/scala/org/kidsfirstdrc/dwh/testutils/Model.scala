package org.kidsfirstdrc.dwh.testutils

object Model {

  case class Genotype(calls: Array[Int])

  val hom_00: Genotype = Genotype(Array(0, 0))
  val hom_11: Genotype = Genotype(Array(1, 1))
  val het_01: Genotype = Genotype(Array(0, 1))
  val het_10: Genotype = Genotype(Array(1, 0))

  val unk: Genotype = Genotype(Array(-1, 0))

  case class GnomadFrequencyEntry(chromosome: String = "2",
                                  start: Long = 165310406,
                                  reference: String = "G",
                                  alternate: String = "A",
                                  ac: Long = 10,
                                  an: Long = 20,
                                  af: BigDecimal = 0.5,
                                  hom: Long = 10)

  case class FrequencyEntry(chromosome: String = "2",
                            start: Long = 165310406,
                            end: Long = 165310406,
                            reference: String = "G",
                            alternate: String = "A",
                            ac: Long = 10,
                            an: Long = 20,
                            af: BigDecimal = 0.5,
                            homozygotes: Long = 10,
                            heterozygotes: Long = 10)

  case class ClinvarEntry(chromosome: String = "2",
                          start: Long = 165310406,
                          end: Long = 165310406,
                          reference: String = "G",
                          alternate: String = "A",
                          name: String = "RCV000436956",
                          clin_sig: String = "Pathogenic")

  case class DBSNPEntry(chromosome: String = "2",
                        start: Long = 165310406,
                        end: Long = 165310406,
                        reference: String = "G",
                        alternate: String = "A",
                        name: String = "rs1234567")

  //case class ParticipantInput(kf_id: String = "PT_001",
  //                            family_id: String = "FM_001",
  //                            affected_status: String = "alive")

  case class BiospecimenInput(kf_id: String = "BS_0001",
                              participant_id: String = "PT_001")

  case class ParticipantOutput(kf_id: String = "PT_001",
                               family_id: String = "FM_001",
                               affected_status: String = "alive",
                               study_id: String = "SD_123",
                               release_id: String = "RE_ABCDEF")

  case class BiosepecimenOutput(kf_id: String = "BS_0001",
                                biospecimen_id: String = "BS_0001",
                                participant_id: String = "PT_001",
                                family_id: String = "FM_001",
                                study_id: String = "SD_123",
                                release_id: String = "RE_ABCDEF")

}
