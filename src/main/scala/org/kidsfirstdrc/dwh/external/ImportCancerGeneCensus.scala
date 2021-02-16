package org.kidsfirstdrc.dwh.external

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.{Public, Raw}
import org.kidsfirstdrc.dwh.conf.DataSource
import org.kidsfirstdrc.dwh.conf.Environment.Environment
import org.kidsfirstdrc.dwh.jobs.DataSourceEtl

class ImportCancerGeneCensus(runEnv: Environment) extends DataSourceEtl(runEnv) {

  override val destination: DataSource = Public.cosmic_gene_set

  override def extract()(implicit spark: SparkSession): Map[DataSource, DataFrame] = {
    Map(Raw.cosmic_cancer_gene_census -> spark.read.option("header", "true").csv(Raw.cosmic_cancer_gene_census.path))
  }

  override def transform(data: Map[DataSource, DataFrame])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    data(Raw.cosmic_cancer_gene_census)
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
  }

  override def load(data: DataFrame)(implicit spark: SparkSession): DataFrame = {
    super.load(data.coalesce(1))
  }
}
