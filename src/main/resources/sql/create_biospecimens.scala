import org.apache.spark.sql.functions
def write_biospecimens(studyId:String, releaseId:String)={
  val raw_bio =  spark.read.parquet(s"s3://kf-variant-parquet-prd/raw/dataservice/biospecimens/biospecimens_${studyId}_${releaseId}")

  val raw_part =  spark.read.parquet(s"s3://kf-variant-parquet-prd/raw/dataservice/participants/participants_${studyId}_${releaseId}")
  // raw_part.printSchema
  // raw_bio.printSchema
  val df = raw_bio.join(raw_part, raw_bio("participant_id") === raw_part("participant_id")).select(raw_bio("*"), raw_part("family_id") as "family_id")
  val releaseIdLc = releaseId.toLowerCase
  val studyIdLc = studyId.toLowerCase
  df
    .coalesce(1)
    .write
    .mode("overwrite")
    .partitionBy("study_id", "release_id")
    .format("parquet")
    .option("path",s"s3://kf-variant-parquet-prd/biospecimens/biospecimens_${studyIdLc}_${releaseIdLc}")
    .saveAsTable(s"variant.biospecimens_${studyIdLc}_${releaseIdLc}")
}

write_biospecimens("SD_9PYZAHHE","RE_123456")

import java.io.{StringWriter,PrintWriter}
def writeAll(studyId:String, releaseId:String)= {
  Seq("participants","outcomes","investigators","genomic_files","family_relationships","families", "diagnoses", "sequencing_experiments", "studies", "outcomes", "study_files", "phenotypes").foreach{name =>
    println(s"Running $name")
    try{
      val releaseIdLc = releaseId.toLowerCase
      val studyIdLc = studyId.toLowerCase
      val path = s"s3://kf-variant-parquet-prd/raw/dataservice/${name}/${name}_${studyId}_${releaseId}"
      spark.read.parquet(path)
        .coalesce(1)
        .write
        .partitionBy("study_id", "release_id")
        .format("parquet")
        .mode("overwrite")
        .option("path",s"s3://kf-variant-parquet-prd/${name}/${name}_${studyIdLc}_${releaseIdLc}")
        .saveAsTable(s"variant.${name}_${studyIdLc}_${releaseIdLc}")
    }catch{
      case e:Exception =>
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        println(sw.toString)
    }
  }
}

writeAll("SD_9PYZAHHE","RE_123456")