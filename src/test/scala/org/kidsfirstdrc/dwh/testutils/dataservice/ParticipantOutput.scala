package org.kidsfirstdrc.dwh.testutils.dataservice

case class ParticipantOutput(
    `affected_status`: Boolean = true,
    `alias_group`: Option[String] = None,
    `biospecimens`: List[String] = List(),
    `created_at`: String = "2018-06-12T17:18:43.597917+00:00",
    `diagnoses`: List[String] = List(),
    `diagnosis_category`: String = "Structural Birth Defect",
    `ethnicity`: String = "Not Hispanic or Latino",
    `external_id`: String = "IA0937",
    `family_id`: String = "FM_4P00M65H",
    `gender`: String = "Male",
    `is_proband`: Boolean = true,
    `kf_id`: String = "PT_QMDEXGH9",
    `modified_at`: String = "2019-04-08T14:42:39.361445+00:00",
    `outcomes`: List[String] = List(),
    `phenotypes`: List[String] = List(),
    `race`: String = "White",
    `study_id`: String = "SD_9PYZAHHE",
    `visible`: Boolean = true,
    `release_id`: String = "RE_000008"
)
