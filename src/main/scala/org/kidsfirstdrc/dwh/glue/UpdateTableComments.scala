package org.kidsfirstdrc.dwh.glue

import bio.ferlab.datalake.spark3.config.{Configuration, DatasetConf, StorageConf}
import org.apache.spark.sql.SparkSession
import org.kidsfirstdrc.dwh.conf.Catalog
import org.kidsfirstdrc.dwh.conf.Catalog.Public.{clinvar, orphanet_gene_set}

import scala.util.{Failure, Success, Try}

object UpdateTableComments extends App {

  val Array(jobType, runEnv) = args
  implicit val spark: SparkSession = SparkSession.builder
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .appName(s"Update table comments - $jobType").getOrCreate()

  implicit val conf: Configuration = Configuration(
    List(StorageConf("kf-strides-variant", "s3a://kf-strides-variant-parquet-prd")),
    sources = Catalog.sources.toList
  )


  jobType match {
    case "all" => Set(clinvar, orphanet_gene_set).foreach(t => run(t))
    case s: String =>
      val names = s.split(",")
      Catalog.sources.filter(ds => names.contains(ds.id)).foreach(t => run(t))
  }

  def run(table: DatasetConf)(implicit spark: SparkSession): Unit = {
    run(table.table.get.database, table.table.get.name, table.documentationpath)
  }

  def run(database: String, table: String, metadata_file: String)(implicit spark: SparkSession): Unit = {
    Try {
      spark.read.option("multiline", "true").json(metadata_file).drop("data_type")
    }.fold(_ => println(s"[ERROR] documentation ${metadata_file} not found."),
      documentationDf => {
         import spark.implicits._
         val describeTableDF = spark.sql(s"DESCRIBE $database.$table")
         val comments = describeTableDF.drop("comment").join(documentationDf, Seq("col_name"))
           .as[GlueFieldComment].collect()

        setComments(comments, database, table)
      }
    )
  }

  def setComments(comments: Array[GlueFieldComment], database: String, table: String)(implicit spark: SparkSession): Unit = {
    comments.foreach {
      case GlueFieldComment(name, _type, comment) =>
        val stmt = s"""ALTER TABLE $database.$table CHANGE $name $name ${_type} COMMENT '${comment.take(255)}' """
        Try(spark.sql(stmt)) match {
          case Failure(_) => println(s"[ERROR] sql statement failed: $stmt")
          case Success(_) => println(s"[INFO] updating comment: $stmt")
        }

    }
  }

  def clearComments(database: String, table: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    val emptyComments = spark.sql(s"DESCRIBE $database.$table").as[GlueFieldComment].collect().map(_.copy(comment = ""))
    setComments(emptyComments, database, table)
  }

}
