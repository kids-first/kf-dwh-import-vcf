package org.kidsfirstdrc.dwh.glue

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf, StorageConf}
import org.apache.spark.sql.SparkSession
import org.kidsfirstdrc.dwh.conf.Catalog
import org.kidsfirstdrc.dwh.conf.Catalog.Clinical.{consequences, variants}
import org.kidsfirstdrc.dwh.conf.Catalog.Public._

import scala.util.{Failure, Success, Try}

object UpdateTableComments extends App {

  val Array(jobType) = args
  implicit val spark: SparkSession = SparkSession.builder
    .config(
      "hive.metastore.client.factory.class",
      "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
    )
    .enableHiveSupport()
    .appName(s"Update table comments - $jobType")
    .getOrCreate()

  implicit val conf: Configuration = Configuration(
    List(StorageConf("kf-strides-variant", "s3a://kf-strides-variant-parquet-prd")),
    sources = Catalog.sources.toList.map(s => s.copy(documentationpath = Some(s"s3a://kf-strides-variant-parquet-prd/jobs/documentation/${s.id}.json")))
  )

  val ids = Set(
    `1000_genomes`, clinvar, consequences, cosmic_gene_set, dbsnp, ddd_gene_set, genes,
    gnomad_genomes_3_1_1, omim_gene_set, orphanet_gene_set, topmed_bravo, variants)
    .map(_.id)

  jobType match {
    case "all" => ids.foreach(id => run(conf.getDataset(id)))
    case s: String => s.split(",").foreach(id => run(conf.getDataset(id)))
  }

  def run(ds: DatasetConf)(implicit spark: SparkSession): Unit = {
    (ds.table, ds.view, ds.documentationpath) match {
      case (None, Some(v), None) =>
        run(v.database, v.name, "")
      case (Some(t), None, Some(documentationpath)) =>
        run(t.database, t.name, documentationpath)
      case (Some(t), Some(v), Some(documentationpath)) =>
        run(t.database, t.name, documentationpath)
        run(v.database, v.name, documentationpath)
      case (Some(t), Some(v), None) =>
        run(t.database, t.name, "")
        run(v.database, v.name, "")
      case (None, None, None) =>
    }
  }

  def run(database: String, table: String, metadata_file: String)(implicit
      spark: SparkSession
  ): Unit = {
    Try {
      spark.read.option("multiline", "true").json(metadata_file).drop("data_type")
    }.fold(
      _ => println(s"[ERROR] documentation ${metadata_file} not found."),
      documentationDf => {
        import spark.implicits._
        val describeTableDF = spark.sql(s"DESCRIBE $database.$table")
        val comments = describeTableDF
          .drop("comment")
          .join(documentationDf, Seq("col_name"))
          .as[GlueFieldComment]
          .collect()

        setComments(comments, database, table)
      }
    )
  }

  def setComments(comments: Array[GlueFieldComment], database: String, table: String)(implicit
      spark: SparkSession
  ): Unit = {
    comments.foreach { case GlueFieldComment(name, _type, comment) =>
      val stmt =
        s"""ALTER TABLE $database.$table CHANGE $name $name ${_type} COMMENT '${comment.take(
          255
        )}' """
      Try(spark.sql(stmt)) match {
        case Failure(_) => println(s"[ERROR] sql statement failed: $stmt")
        case Success(_) => println(s"[INFO] updating comment: $stmt")
      }

    }
  }

  def clearComments(database: String, table: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    val emptyComments = spark
      .sql(s"DESCRIBE $database.$table")
      .as[GlueFieldComment]
      .collect()
      .map(_.copy(comment = ""))
    setComments(emptyComments, database, table)
  }

}
