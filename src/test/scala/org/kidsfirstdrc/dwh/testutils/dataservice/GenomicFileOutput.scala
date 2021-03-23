package org.kidsfirstdrc.dwh.testutils.dataservice

case class GenomicFileOutput(`acl`: List[String] = List("phs001168.c1", "SD_9PYZAHHE"),
                             `access_urls`: List[String] = List("https://data.kidsfirstdrc.org/data/a6bccb21-5b16-4ceb-bb61-6416212eed56"),
                             `availability`: String = "Immediate Download",
                             `controlled_access`: Boolean = true,
                             `created_at`: String = "2018-06-12T18:03:37.397813+00:00",
                             `data_type`: String = "Aligned Reads",
                             `external_id`: String = "kf-study-us-east-1-prd-sd-9pyzahhe/harmonized/cram/15432d95-83da-4c55-be58-55e9e4ec587b.cram",
                             `file_format`: String = "cram",
                             `file_name`: String = "15432d95-83da-4c55-be58-55e9e4ec587b.cram",
                             `hashes`: HASHES = HASHES(),
                             `instrument_models`: List[String] = List(),
                             `is_harmonized`: Boolean = true,
                             `is_paired_end`: Option[Boolean] = None,
                             `kf_id`: String = "GF_685J0QSN",
                             `latest_did`: String = "a6bccb21-5b16-4ceb-bb61-6416212eed56",
                             `metadata`: METADATA = METADATA(),
                             `modified_at`: String = "2019-04-09T16:34:55.131262+00:00",
                             `platforms`: List[String] = List(),
                             `reference_genome`: String = "GRCh38",
                             `repository`: Option[String] = None,
                             `size`: Long = 16818312573l,
                             `urls`: List[String] = List("s3://kf-study-us-east-1-prd-sd-9pyzahhe/harmonized/cram/15432d95-83da-4c55-be58-55e9e4ec587b.cram"),
                             `visible`: Boolean = true,
                             `study_id`: String = "SD_9PYZAHHE",
                             `release_id`: String = "RE_000008")

case class HASHES(`md5`: Option[String] = None)


case class METADATA(`placeholder`: Option[String] = None)
