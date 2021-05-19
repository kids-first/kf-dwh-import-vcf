package org.kidsfirstdrc.dwh.external

import bio.ferlab.datalake.spark3.config.Configuration
import bio.ferlab.datalake.spark3.config.DatasetConf
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{split, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.{Public, Raw}

import org.kidsfirstdrc.dwh.jobs.StandardETL

class ImportHumanGenes()(implicit conf: Configuration)
  extends StandardETL(Public.human_genes)(conf) {

  override def extract()(implicit spark: SparkSession): Map[DatasetConf, DataFrame] = {
    val df = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .option("sep", "\t")
      .option("nullValue", "-")
      .load(Raw.refseq_homo_sapiens_gene.location)
    Map(Raw.refseq_homo_sapiens_gene -> df)
  }

  override def transform(data: Map[DatasetConf, DataFrame])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    data(Raw.refseq_homo_sapiens_gene)
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

  }

  override def load(data: DataFrame)(implicit spark: SparkSession): DataFrame = {
    super.load(data.coalesce(1))
  }

  val splitToMapFn : String => Option[Map[String, String]] = {line =>
    Option(line)
      .map {l =>
        val elements = l.split("\\|")
        val m = elements.map { e =>
          val Array(key,value) = e.split(":", 2)
          key.toLowerCase.replaceAll("/", "_").replaceAll("-", "_") -> value
        }
        m.toMap
      }
  }

  val splitToMap: UserDefinedFunction = udf(splitToMapFn)
}
