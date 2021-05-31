package org.kidsfirstdrc.dwh.sql

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import java.net.URI
import scala.collection.convert.ImplicitConversions._
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

object ExportCreateTables extends App {

  val Array(schema, outputS3a, extras) = args

  implicit val spark: SparkSession = SparkSession.builder
    .config(
      "hive.metastore.client.factory.class",
      "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
    )
    .enableHiveSupport()
    .appName(s"Export $schema create table to $outputS3a")
    .getOrCreate()

  import spark.implicits._

  val tables: Array[String] =
    spark.sql(s"SHOW TABLES in $schema").select("tableName").as[String].collect() ++ extras.split(
      ","
    )

  private def extractPart(
      folder: String,
      currentExtention: String,
      newExtension: String
  ): Boolean = {
    val fs    = FileSystem.get(URI.create(folder), new Configuration())
    val it    = fs.listFiles(new Path(folder), true)
    val files = new ArrayBuffer[String]()
    while (it.hasNext) {
      val lfs = it.next
      files.add(lfs.getPath.getName)
    }

    val partFile = files.toList.filter(_.endsWith(s".${currentExtention}")).head

    println(s"$folder/$partFile")
    println(s"-> ${folder}.${newExtension}")

    fs.rename(new Path(s"$folder/$partFile"), new Path(s"${folder}.${newExtension}"))
    fs.delete(new Path(folder), true)
  }

  tables.foreach(t =>
    Try {
      val outputS3 = outputS3a.replace("s3a://", "s3://")
      val pathS3   = s"$outputS3/variant_${t}"
      val pathS3a  = s"$outputS3a/variant_${t}"
      spark
        .sql(s"SHOW CREATE TABLE $schema.$t")
        .select("createtab_stmt")
        .as[String]
        .limit(1)
        .coalesce(1) //better be safe than sorry - ensures Spark writes only one file
        .write
        .mode("overwrite")
        .text(pathS3a)

      extractPart(pathS3, "txt", "sql")

    }.fold(e => println(e.getMessage), _ => println("DONE"))
  )

}
