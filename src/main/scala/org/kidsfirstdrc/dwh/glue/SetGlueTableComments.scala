package org.kidsfirstdrc.dwh.glue

import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

object SetGlueTableComments {

  def run(database: String, table: String, metadata_file: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    val commentsDF = spark.read.option("multiline", "true").json(metadata_file).drop("data_type")

    val describeTableDF = spark.sql(s"DESCRIBE $database.$table")

    val comments = describeTableDF.drop("comment").join(commentsDF, Seq("col_name"))
      .as[GlueFieldComment].collect()

    setComments(comments, database, table)
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

case class GlueFieldComment(col_name: String,
                            data_type: String,
                            comment: String)
