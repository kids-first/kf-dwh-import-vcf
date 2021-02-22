package org.kidsfirstdrc.dwh.variantDb.stats

import org.apache.http.HttpHeaders
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write


object Stats extends App {
  val Array(es_host) = args

  val DATABASE = "variant"

  implicit val formats: DefaultFormats.type = DefaultFormats

  implicit val spark: SparkSession = SparkSession.builder
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .appName("Index Variant Stats").getOrCreate()

  val occurrencesTables = StatsUtils.getOccurrencesTableWORelease(DATABASE)
  val occurrencesDF = StatsUtils.getUnionOfOccurrences(DATABASE, occurrencesTables)
  val stats = StatsUtils.getStats(occurrencesDF)

  val client = HttpClientBuilder.create().build()

  val post = new HttpPost(s"${es_host}/variant_stats/doc/1")

  post.addHeader(HttpHeaders.CONTENT_TYPE,"application/json")

  post.setEntity(new StringEntity(write(stats)))

  try {
    client.execute(post)
  } finally {
    client.close()
  }
}
