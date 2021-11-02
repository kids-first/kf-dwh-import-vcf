package org.kidsfirstdrc.dwh.es.stats

import org.apache.commons.io.IOUtils
import org.apache.http.HttpHeaders
import org.apache.http.client.methods.{CloseableHttpResponse, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{DefaultHttpClient, HttpClientBuilder}
import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write

import java.nio.charset.StandardCharsets

object Stats extends App {
  val Array(es_host) = args

  val DATABASE = "variant"

  implicit val formats: DefaultFormats.type = DefaultFormats

  implicit val spark: SparkSession = SparkSession.builder
    .config(
      "hive.metastore.client.factory.class",
      "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
    )
    .enableHiveSupport()
    .appName("Index Variant Stats")
    .getOrCreate()

  val occurrencesTables = StatsUtils.getOccurrencesTableWORelease(DATABASE)
  val occurrencesDF     = StatsUtils.getUnionOfOccurrences(DATABASE, occurrencesTables)
  val stats             = StatsUtils.getStats(occurrencesDF)

  val client = new DefaultHttpClient()

  val post = new HttpPost(s"${es_host}/variant_stats/_doc/1")

  post.addHeader(HttpHeaders.CONTENT_TYPE, "application/json")

  post.setEntity(new StringEntity(write(stats)))

  val response: CloseableHttpResponse = client.execute(post)
  if(! Seq(200, 201).contains(response.getStatusLine.getStatusCode )){
      val body = IOUtils.toString(response.getEntity.getContent, StandardCharsets.UTF_8)
      throw new IllegalStateException(s"Error in response statusCode=${response.getStatusLine.getStatusCode}, body=${body}" )
  }
}
