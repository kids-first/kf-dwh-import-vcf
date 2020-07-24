val allStudies = Seq("SD_46SK55A3","SD_6FPYJQBR","SD_9PYZAHHE","SD_DYPMEHHF","SD_DZTB5HRR","SD_PREASA7S","SD_R0EPRSGS","SD_YGVA0E1C")
allStudies
  .foreach{ study =>
    val studyLc = study.toLowerCase
    spark.sql(s"CREATE OR REPLACE VIEW variant.occurrences_${studyLc} AS SELECT * FROM variant.occurrences_${studyLc}_re_000004")

  }

