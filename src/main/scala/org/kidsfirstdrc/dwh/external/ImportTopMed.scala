package org.kidsfirstdrc.dwh.external

import bio.ferlab.datalake.commons.config.Configuration
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.{Public, Raw}
import org.kidsfirstdrc.dwh.jobs.StandardETL

import java.time.LocalDateTime

class ImportTopMed()(implicit conf: Configuration) extends StandardETL(Public.topmed_bravo)(conf) {

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(Raw.topmed_bravo_dbsnp.id -> vcf(Raw.topmed_bravo_dbsnp.location, None)(spark))
  }

  override def transform(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    data(Raw.topmed_bravo_dbsnp.id)
      .select(
        chromosome,
        start,
        end,
        name,
        reference,
        alternate,
        ac,
        af,
        an,
        $"INFO_HOM" (0) as "homozygotes",
        $"INFO_HET" (0) as "heterozygotes",
        $"qual",
        $"INFO_FILTERS" as "filters",
        when(size($"INFO_FILTERS") === 1 && $"INFO_FILTERS" (0) === "PASS", "PASS")
          .when(array_contains($"INFO_FILTERS", "PASS"), "PASS+FAIL")
          .otherwise("FAIL") as "qual_filter"
      )
  }

  override def load(data: DataFrame,
                    lastRunDateTime: LocalDateTime = minDateTime,
                    currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    super.load(
      data
        .repartition(col("chromosome"))
        .sortWithinPartitions("start")
    )
  }
}
