import org.apache.spark.sql.functions
spark.read.parquet("s3://kf-variant-parquet-prd/participantcentric").withColumn("biospecimen", functions.explode($"biospecimens")  )
  .select($"biospecimen.*", $"kf_id" as "participant_id",$"family_id", $"ethnicity", $"external_id" as "participant_external_id", $"affected_status", $"gender", $"is_proband", $"race", $"biospecimen.diagnoses.kf_id" as "histological_diagnoses_ids", $"study.kf_id" as "study_id", lit("RE_123456") as "release_id", $"study")
  .drop("diagnoses")
  .withColumnRenamed("kf_id", "biospecimen_id")
  .write
  .partitionBy("study_id", "release_id")
  .format("parquet")
  .option("path","s3://kf-variant-parquet-prd/biospecimens/biospecimens_sd_9pyzahhe_re_123456")
  .saveAsTable("variant.biospecimens_sd_9pyzahhe_re_123456")