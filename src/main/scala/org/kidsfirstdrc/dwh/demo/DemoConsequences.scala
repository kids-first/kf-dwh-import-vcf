package org.kidsfirstdrc.dwh.demo

import bio.ferlab.datalake.core.config.{Configuration, StorageConf}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{input_file_name, regexp_extract}
import org.kidsfirstdrc.dwh.conf.Catalog.HarmonizedData
import org.kidsfirstdrc.dwh.utils.SparkUtils._
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns._
import org.kidsfirstdrc.dwh.vcf.Consequences

object DemoConsequences {

  implicit val conf: Configuration = Configuration(List(
    StorageConf("kf-strides-variant", "s3a://kf-strides-variant-parquet-prd/public/demo")
  ))

  def run(studyId: String, releaseId: String, input: String, output: String)(implicit spark: SparkSession): Unit = {

    val inputDF = vcf(input)
      .withColumn("file_name", regexp_extract(input_file_name(), ".*/(.*)", 1))
      .select(chromosome, start, end, reference, alternate, name, annotations)

    val data = Map(HarmonizedData.family_variants_vcf -> inputDF)

    val consequences = new Consequences(studyId, releaseId, input).transform(data)

    new Consequences(studyId, releaseId, input).load(consequences)
  }
}
