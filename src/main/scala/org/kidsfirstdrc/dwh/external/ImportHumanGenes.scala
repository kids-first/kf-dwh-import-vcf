package org.kidsfirstdrc.dwh.external

import bio.ferlab.datalake.commons.config.Configuration
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{split, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.{Public, Raw}
import org.kidsfirstdrc.dwh.jobs.StandardETL

import java.time.LocalDateTime

class ImportHumanGenes()(implicit conf: Configuration)
    extends StandardETL(Public.human_genes)(conf) {

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    val df = spark.read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .option("sep", "\t")
      .option("nullValue", "-")
      .load(Raw.refseq_homo_sapiens_gene.location)
    Map(Raw.refseq_homo_sapiens_gene.id -> df)
  }

  override def transform(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    data(Raw.refseq_homo_sapiens_gene.id)
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

  override def load(data: DataFrame,
                    lastRunDateTime: LocalDateTime = minDateTime,
                    currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    super.load(data.coalesce(1))
  }

  val splitToMapFn: String => Option[Map[String, String]] = { line =>
    Option(line)
      .map { l =>
        val elements = l.split("\\|")
        val m = elements.map { e =>
          val Array(key, value) = e.split(":", 2)
          key.toLowerCase.replaceAll("/", "_").replaceAll("-", "_") -> value
        }
        m.toMap
      }
  }

  val splitToMap: UserDefinedFunction = udf(splitToMapFn)
}
