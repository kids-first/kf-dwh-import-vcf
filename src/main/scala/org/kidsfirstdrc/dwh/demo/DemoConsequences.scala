package org.kidsfirstdrc.dwh.demo

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf, RunStep}
import bio.ferlab.datalake.spark3.etl.ETL
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns._
import org.apache.spark.sql.functions.{input_file_name, regexp_extract}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog
import org.kidsfirstdrc.dwh.conf.Catalog.HarmonizedData
import org.kidsfirstdrc.dwh.vcf.Consequences

import java.time.LocalDateTime

class DemoConsequences(studyId: String, releaseId: String, input: String)(implicit
    conf: Configuration
) extends ETL() {

  override def run(runSteps: Seq[RunStep] = RunStep.default_load,
          lastRunDateTime: Option[LocalDateTime] = None,
          currentRunDateTime: Option[LocalDateTime] = None)(implicit spark: SparkSession): DataFrame = {
    val data         = extract()
    val consequences = transform(data)
    load(consequences)
  }

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    val df = vcf(input, None)
      .withColumn("file_name", regexp_extract(input_file_name(), ".*/(.*)", 1))
      .select(chromosome, start, end, reference, alternate, name, annotations)
    Map(HarmonizedData.family_variants_vcf.id -> df)
  }

  override def transform(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    new Consequences(studyId, releaseId, "", "").transform(data)
  }

  override def load(data: DataFrame,
                    lastRunDateTime: LocalDateTime = minDateTime,
                    currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    new Consequences(studyId, releaseId, "", "").load(data)
  }

  override val destination: DatasetConf = Catalog.Clinical.consequences
}
