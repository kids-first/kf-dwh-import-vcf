val all = Set("participants", "biospecimens", "biospecimens_diagnoses", "outcomes", "investigators", "genomic_files", "family_relationships", "families", "diagnoses", "sequencing_experiments", "studies", "outcomes", "study_files", "phenotypes")
val release = "RE_000004"
all.foreach{ t=>
  spark.sql(s"create or replace view variant_live.$t as select * from variant.${t}_${release.toLowerCase()}")

}
