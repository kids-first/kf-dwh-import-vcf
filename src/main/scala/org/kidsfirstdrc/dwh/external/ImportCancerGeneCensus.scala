package org.kidsfirstdrc.dwh.external

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions._

object ImportCancerGeneCensus extends App {
  implicit val spark: SparkSession = SparkSession.builder
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .appName("Import Cosmic Cancer Gene Census").getOrCreate()

  import spark.implicits._

  val input = "s3a://kf-variant-parquet-prd/raw/cosmic/cancer_gene_census.csv"
  val output = "s3a://kf-variant-parquet-prd/public"
  spark.read.option("header", "true").csv(input)
    .select(
      $"Gene Symbol" as "symbol",
      $"Name" as "name",
      $"Entrez GeneId" as "entrez_gene_id",
      $"Tier".cast(IntegerType),
      $"Genome Location" as "genome_location",
      when($"Hallmark" === "Yes", true).otherwise(false) as "hallmark",
      $"Chr Band" as "chr_band",
      when($"Somatic" === "yes", true).otherwise(false) as "somatic",
      when($"Germline" === "yes", true).otherwise(false) as "germline",
      split($"Tumour Types(Somatic)", ",") as "tumour_types_somatic",
      split($"Tumour Types(Germline)", ",") as "tumour_types_germline",
      $"Cancer Syndrome" as "cancer_syndrome",
      split($"Tissue Type", ",") as "tissue_type",
      $"Molecular Genetics" as "molecular_genetics",
      split($"Role in Cancer", ",") as "role_in_cancer",
      split($"Mutation Types", ",") as "mutation_types",
      split($"Translocation Partner", ",") as "translocation_partner",
      when($"Other Germline Mut" === "yes", true).otherwise(false) as "other_germline_mutation",
      split($"Other Syndrome", ",") as "other_syndrome",
      split($"Synonyms", ",") as "synonyms"
    )
    .write
    .mode("overwrite")
    .format("parquet")
    .option("path", s"$output/cosmic_gene_set")
    .saveAsTable("variant_live.cosmic_gene_set")

}
