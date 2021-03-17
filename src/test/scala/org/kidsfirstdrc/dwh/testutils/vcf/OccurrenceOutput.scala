package org.kidsfirstdrc.dwh.testutils.vcf

case class OccurrenceOutput(chromosome: String = "2",
                            start: Long = 165310406,
                            end: Long = 165310406,
                            reference: String = "G",
                            alternate: String = "A",
                            name: Option[String] = Some("rs112766696"),
                            biospecimen_id: String = "BS_HIJKKL",
                            participant_id: String = "PT_000003",
                            family_id: Option[String] = Some("FA_000002"),
                            study_id: String = "SD_789",
                            release_id: String = "re_000010",
                            file_name: String = "file1",
                            dbgap_consent_code: String = "c1",
                            is_gru: Boolean = false,
                            is_hmb: Boolean = false)