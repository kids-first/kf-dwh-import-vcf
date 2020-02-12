CREATE EXTERNAL TABLE occurences(
    `reference` STRING,
    `start` BIGINT,
    `end` BIGINT,
    `alternate` STRING,
    `biospecimen_id` STRING,
    `family_id` STRING,


    )
PARTITIONED BY (`studyId` STRING, `releaseId` STRING,  `chromosome` STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
'serialization.format' = '1'
)
STORED AS
  INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://kf-variant-parquet-prd/occurences/occurences_sd_9pyzahhe_re_123456'
TBLPROPERTIES (
  'has_encrypted_data' = 'false'
)

/*`participant_id` STRING, `quality` DOUBLE, `info_lo_conf_denovo` STRING, `info_hi_conf_denovo` STRING, `info_negative_train_site` BOOLEAN, `info_positive_train_site` BOOLEAN, `info_ac` ARRAY<INT>, `info_an` INT, `info_af` ARRAY<DOUBLE>, `info_culprit` STRING, `info_sor` DOUBLE, `info_read_pos_rank_sum` ARRAY<DOUBLE>, `info_in_breeding_coeff` DOUBLE, `info_pg` ARRAY<INT>, `info_fs` DOUBLE, `info_dp` INT, `sampleid` STRING, `conditionalquality` INT, `filters` ARRAY<STRING>, `sb` ARRAY<INT>, `alleledepths` ARRAY<INT>, `pp` ARRAY<INT>, `pid` ARRAY<STRING>, `phased` BOOLEAN, `calls` ARRAY<INT>, `min_dp` ARRAY<INT>, `jl` INT, `pgt` ARRAY<STRING>, `phredlikelihoods` ARRAY<INT>, `depth` INT, `rgq` INT, `jp` INT, `hgvsg` STRING*/