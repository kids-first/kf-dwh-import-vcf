package org.kidsfirstdrc.dwh.testutils.vcf

case class OccurrenceOutput(chromosome: String,
                            start: Long,
                            end: Long,
                            reference: String,
                            alternate: String,
                            name: Option[String],
                            biospecimen_id: String,
                            participant_id: String,
                            family_id: Option[String],
                            study_id: String,
                            release_id: String,
                            file_name: String,
                            dbgap_consent_code: String,
                            is_gru: Boolean = false,
                            is_hmb: Boolean = false)
