package org.kidsfirstdrc.dwh

package object external {

  object Dataservice {
    val ALL_TABLES = Set(
      "participants",
      "biospecimens",
      "biospecimens_diagnoses",
      "outcomes",
      "investigators",
      "genomic_files",
      "family_relationships",
      "families",
      "diagnoses",
      "sequencing_experiments",
      "studies",
      "outcomes",
      "study_files",
      "phenotypes",
      "genomic_files"
    )
  }

}
