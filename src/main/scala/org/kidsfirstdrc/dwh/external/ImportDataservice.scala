package org.kidsfirstdrc.dwh.external

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object ImportDataservice extends App {

  val (studyIds, releaseId, input, output, mergeExisting, tables) = parseArgs(args)

  implicit val spark: SparkSession = SparkSession.builder
    .config(
      "hive.metastore.client.factory.class",
      "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
    )
    .enableHiveSupport()
    .appName("Import Dataservice")
    .getOrCreate()

  build(studyIds, releaseId, input, output, mergeExisting, tables)

  def parseArgs(
      args: Array[String]
  ): (Set[String], String, String, String, Boolean, Set[String]) = {
    def multi(s: String): Set[String] = s.split(",").toSet

    val Array(studyIds, releaseId, input, output, mergeExisting) = args.take(5)
    val tables =
      if (args.length == 6 && args(5) != "all") multi(args(5))
      else {
        Dataservice.ALL_TABLES
      }
    (multi(studyIds), releaseId, input, output, mergeExisting.toBoolean, tables)
  }

  def build(
      studyIds: Set[String],
      releaseId: String,
      input: String,
      output: String,
      mergeExisting: Boolean,
      tables: Set[String]
  )(implicit spark: SparkSession): Unit = {
    (tables - "biospecimens").foreach { name =>
      write(studyIds, releaseId, input, output, mergeExisting, name)
    }
    if (tables.contains("biospecimens")) {
      writeLoad(studyIds, releaseId, output, mergeExisting, "biospecimens") { (studyId, spark) =>
        import spark.implicits._
        val bio  = spark.read.parquet(s"$input/biospecimens/$studyId")
        val part = spark.read.parquet(s"$input/participants/$studyId")
        val df = bio
          .join(part, bio("participant_id") === part("kf_id"))
          .select(bio("*"), part("family_id") as "family_id")
        df
          .withColumn("biospecimen_id", $"kf_id")
      }
    }
  }

  def write(
      studyIds: Set[String],
      releaseId: String,
      input: String,
      output: String,
      mergeExisting: Boolean,
      name: String
  )(implicit spark: SparkSession): Unit = {
    writeLoad(studyIds, releaseId, output, mergeExisting, name) { (studyId, spark) =>
      spark.read.parquet(s"$input/$name/$studyId")
    }
  }

  def writeLoad(
      studyIds: Set[String],
      releaseId: String,
      output: String,
      mergeExisting: Boolean,
      name: String
  )(load: (String, SparkSession) => DataFrame)(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    val releaseIdLc = releaseId.toLowerCase
    val unionsDF = studyIds.foldLeft(spark.emptyDataFrame) { (currentDF, studyId) =>
      val nextDf: DataFrame = load(studyId, spark)
        .withColumn("study_id", lit(studyId))
        .withColumn("release_id", lit(releaseId))
      if (currentDF.isEmpty) {
        nextDf
      } else {
        currentDF.unionByName(nextDf).dropDuplicates()
      }
    }
    val existingTableName = s"variant.$name"
    val merged = if (mergeExisting && spark.catalog.tableExists(existingTableName)) {
      val existing = spark.table(existingTableName).where(not($"study_id".isin(studyIds.toSeq: _*)))

      existing.unionByName(unionsDF).dropDuplicates()
    } else {
      unionsDF
    }

    merged
      .coalesce(1)
      .write
      .format("parquet")
      .mode("overwrite")
      .option("path", s"$output/$name/${name}_$releaseIdLc")
      .saveAsTable(s"variant.${name}_$releaseIdLc")
  }

}
