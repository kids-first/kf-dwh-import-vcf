package org.kidsfirstdrc.dwh.demo

import bio.ferlab.datalake.spark3.config.{Configuration, SourceConf}
import bio.ferlab.datalake.spark3.etl.ETL
import org.apache.spark.sql.functions.{input_file_name, regexp_extract}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.HarmonizedData
import org.kidsfirstdrc.dwh.utils.SparkUtils._
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns._
import org.kidsfirstdrc.dwh.vcf.Consequences

class DemoConsequences(studyId: String, releaseId: String, input: String)
                      (implicit conf: Configuration)
  extends ETL(){

  override def run()(implicit spark: SparkSession): DataFrame = {
    val data = extract()
    val consequences = transform(data)
    load(consequences)
  }

  override def extract()(implicit spark: SparkSession): Map[SourceConf, DataFrame] = {
    val df = vcf(input)
      .withColumn("file_name", regexp_extract(input_file_name(), ".*/(.*)", 1))
      .select(chromosome, start, end, reference, alternate, name, annotations)
    Map(HarmonizedData.family_variants_vcf -> df)
  }

  override def transform(data: Map[SourceConf, DataFrame])(implicit spark: SparkSession): DataFrame = {
    new Consequences(studyId, releaseId, input, "", "").transform(data)
  }

  override def load(data: DataFrame)(implicit spark: SparkSession): DataFrame = {
    new Consequences(studyId, releaseId, input, "", "").load(data)
  }
}
