package org.kidsfirstdrc.dwh.es.index

import bio.ferlab.datalake.spark3.elasticsearch.{ElasticSearchClient, Indexer}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

object Index extends App {

  println(s"ARGS: " + args.mkString("[", ", ", "]"))

  val Array(
    input,
    esNodes,
    alias,
    oldRelease,
    newRelease,
    templateFileName,
    jobType,
    batchSize,
    chromosome,
    format,
    repartition
  ) = args

  implicit val spark: SparkSession = SparkSession.builder
    .config("es.index.auto.create", "true")
    .config("es.nodes", esNodes)
    .config("es.batch.size.entries", batchSize)
    .config("es.batch.write.retry.wait", "100s")
    .config("es.nodes.client.only", "false")
    .config("es.nodes.discovery", "false")
    .config("es.nodes.wan.only", "true")
    .config("es.read.ignore_exception", "true")
    .config("es.port", "443")
    .config("es.wan.only", "true")
    .config("es.write.ignore_exception", "true")
    .config("spark.es.nodes.client.only", "false")
    .config("spark.es.nodes.wan.only", "true")
    .appName(s"Indexer")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val templatePath = s"s3://kf-strides-variant-parquet-prd/jobs/templates/$templateFileName"

  val indexName = chromosome match {
    case "all" => alias
    case c     => s"${alias}_${c}"
  }

  println(s"$jobType - ${indexName}_$newRelease")

  val job = new Indexer(jobType, templatePath, alias, s"${indexName}_$newRelease", Some(s"${indexName}_$oldRelease"))
  implicit val esClient: ElasticSearchClient = new ElasticSearchClient(esNodes.split(',').head)

  val df: DataFrame = chromosome match {
    case "all" =>
      Try(repartition.toInt).toOption
        .fold {
          spark.read
            .format(format)
            .load(input)
        } { n =>
          spark.read
            .format(format)
            .load(input)
            .repartition(n)
        }

    case s =>
      spark.read
        .format(format)
        .load(input)
        .where(col("chromosome") === s)
  }

  job.run(df)(esClient)
}
