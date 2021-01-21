package org.kidsfirstdrc.dwh.external

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{split, udf}

object ImportRefSeq extends App {

  implicit val spark: SparkSession = SparkSession.builder
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .appName("Import Refseq").getOrCreate()

  import spark.implicits._

  val splitToMapFn : String=>Option[Map[String,String] ]= line => {
    if(line == null)
      None
    else{
      val elements = line.split("\\|")
      val m = elements.map{ e =>
        val Array(key,value) = e.split(":", 2)
        key.toLowerCase.replaceAll("/", "_").replaceAll("-", "_") -> value
      }
      Some(m.toMap)
    }
  }

  val splitToMap = udf(splitToMapFn)

  val input = "s3a://kf-strides-variant-parquet-prd/raw/refseq/Homo_sapiens.gene_info.gz"
  val output = "s3a://kf-strides-variant-parquet-prd/public"
  spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .option("sep", "\t")
    .option("nullValue", "-")
    .load("s3a://kf-strides-variant-parquet-prd/raw/refseq/Homo_sapiens.gene_info.gz")
    .select(
      $"#tax_id" as "tax_id",
      $"GeneID" as "entrez_gene_id",
      $"Symbol" as "symbol",
      $"LocusTag" as "locus_tag",
      split($"Synonyms", "\\|") as "synonyms",
      splitToMap($"dbXrefs") as "external_references",
      $"chromosome",
      $"map_location",
      $"description",
      $"type_of_gene",
      $"Symbol_from_nomenclature_authority" as "symbol_from_nomenclature_authority",
      $"Full_name_from_nomenclature_authority" as "full_name_from_nomenclature_authority",
      $"Nomenclature_status" as "nomenclature_status",
      split($"Other_designations", "\\|") as "other_designations",
      splitToMap($"Feature_type") as "feature_types"
    )
    .withColumn("ensembl_gene_id", $"external_references.ensembl")
    .withColumn("omim_gene_id", $"external_references.mim")
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("parquet")
    .option("path", s"$output/human_genes")
    .saveAsTable("variant.human_genes")
}
