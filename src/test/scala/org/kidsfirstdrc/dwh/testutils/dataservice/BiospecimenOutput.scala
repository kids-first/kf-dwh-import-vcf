package org.kidsfirstdrc.dwh.testutils.dataservice

case class BiospecimenOutput(
    `age_at_event_days`: Int = 3287,
    `analyte_type`: String = "DNA",
    `composition`: String = "Bone Marrow",
    `concentration_mg_per_ml`: Double = 0.1916,
    `consent_type`: String = "GRU",
    `created_at`: String = "2018-11-19T20:28:32.988849+00:00",
    `duo_ids`: List[String] = List(),
    `dbgap_consent_code`: String = "phs001738.c1",
    `external_aliquot_id`: String = "4219-CGM-0377",
    `external_sample_id`: String = "4219-CGM-0377",
    `kf_id`: String = "BS_F9E3S8QT",
    `method_of_sample_procurement`: Option[String] = None,
    `modified_at`: String = "2020-06-19T21:36:37.741370+00:00",
    `ncit_id_anatomical_site`: String = "Not Reported",
    `ncit_id_tissue_type`: String = "Not Reported",
    `shipment_date`: Option[String] = None,
    `shipment_origin`: String = "Not Reported",
    `genomic_files`: List[String] = List(),
    `participant_id`: String = "PT_CVBR1Y3J",
    `source_text_tumor_descriptor`: String = "Not Reported",
    `source_text_tissue_type`: String = "Tumor",
    `source_text_anatomical_site`: String = "Bone Marrow",
    `spatial_descriptor`: String = "Not Reported",
    `uberon_id_anatomical_site`: String = "UBERON:0002371",
    `volume_ml`: Option[Double] = None,
    `sequencing_center_id`: Option[String] = None,
    //`diagnoses`: List[DIAGNOSES] = List(DIAGNOSES()),
    `visible`: Boolean = true,
    `family_id`: String = "FM_AXBX711X",
    `biospecimen_id`: String = "BS_F9E3S8QT",
    `study_id`: String = "SD_W0V965XZ",
    `release_id`: String = "RE_000008"
)
