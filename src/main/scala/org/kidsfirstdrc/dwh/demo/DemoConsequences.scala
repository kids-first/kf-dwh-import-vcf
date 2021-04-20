package org.kidsfirstdrc.dwh.demo

import bio.ferlab.datalake.core.config.Configuration
import bio.ferlab.datalake.core.etl.{DataSource, ETL}
import org.apache.spark.sql.functions.{input_file_name, regexp_extract}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.{Clinical, HarmonizedData}
import org.kidsfirstdrc.dwh.utils.SparkUtils._
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns._
import org.kidsfirstdrc.dwh.vcf.Consequences

class DemoConsequences(studyId: String, releaseId: String, input: String)
                      (implicit conf: Configuration)
  extends ETL(Clinical.consequences){

  override def run()(implicit spark: SparkSession): DataFrame = {
    val data = extract()
    val consequences = transform(data)
    load(consequences)
  }

  override def extract()(implicit spark: SparkSession): Map[DataSource, DataFrame] = {
    val df = vcf(input)
      .withColumn("file_name", regexp_extract(input_file_name(), ".*/(.*)", 1))
      .select(chromosome, start, end, reference, alternate, name, annotations)
    Map(HarmonizedData.family_variants_vcf -> df)
  }

  override def transform(data: Map[DataSource, DataFrame])(implicit spark: SparkSession): DataFrame = {
    new Consequences(studyId, releaseId, input, "", "").transform(data)
  }

  override def load(data: DataFrame)(implicit spark: SparkSession): DataFrame = {
    new Consequences(studyId, releaseId, input, "", "").load(data)
  }
}
