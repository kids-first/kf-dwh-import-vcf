package org.kidsfirstdrc.dwh.external

import bio.ferlab.datalake.core.config.Configuration
import bio.ferlab.datalake.core.etl.DataSource
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.{Public, Raw}
import org.kidsfirstdrc.dwh.conf.Environment.Environment
import org.kidsfirstdrc.dwh.jobs.StandardETL
import org.kidsfirstdrc.dwh.utils.SparkUtils._
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns._

class ImportTopMed(runEnv: Environment)(implicit conf: Configuration)
  extends StandardETL(Public.topmed_bravo)(runEnv, conf) {

  override def extract()(implicit spark: SparkSession): Map[DataSource, DataFrame] = {
    Map(Raw.topmed_bravo_dbsnp -> vcf(Raw.topmed_bravo_dbsnp.location)(spark))
  }

  override def transform(data: Map[DataSource, DataFrame])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    data(Raw.topmed_bravo_dbsnp)
      .select(chromosome,
        start,
        end,
        name,
        reference,
        alternate,
        ac,
        af,
        an,
        $"INFO_HOM"(0) as "homozygotes",
        $"INFO_HET"(0) as "heterozygotes",
        $"qual",
        $"INFO_FILTERS" as "filters",
        when(size($"INFO_FILTERS") === 1 && $"INFO_FILTERS"(0) === "PASS", "PASS").when(array_contains($"INFO_FILTERS", "PASS"), "PASS+FAIL").otherwise("FAIL") as "qual_filter"
      )
  }

  override def load(data: DataFrame)(implicit spark: SparkSession): DataFrame = {
    super.load(
      data
        .repartition(col("chromosome"))
        .sortWithinPartitions("start")
    )
  }
}
