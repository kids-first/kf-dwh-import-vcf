package org.kidsfirstdrc.dwh.external

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.{Public, Raw}
import org.kidsfirstdrc.dwh.conf.Ds
import org.kidsfirstdrc.dwh.conf.Environment.Environment
import org.kidsfirstdrc.dwh.jobs.DsETL
import org.kidsfirstdrc.dwh.utils.SparkUtils._
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns._

class ImportTopMed(runEnv: Environment) extends DsETL(runEnv) {

  override val destination: Ds = Public.topmed_bravo

  override def extract()(implicit spark: SparkSession): Map[Ds, DataFrame] = {
    Map(Raw.topmed_bravo_dbsnp -> vcf(Raw.topmed_bravo_dbsnp.path)(spark))
  }

  override def transform(data: Map[Ds, DataFrame])(implicit spark: SparkSession): DataFrame = {
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
