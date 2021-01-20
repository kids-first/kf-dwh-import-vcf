package org.kidsfirstdrc.dwh.demo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{input_file_name, regexp_extract}
import org.kidsfirstdrc.dwh.utils.SparkUtils._
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns._
import org.kidsfirstdrc.dwh.vcf.Consequences

object DemoConsequences {
  def run(studyId: String, releaseId: String, input: String, output: String)(implicit spark: SparkSession): Unit = {

    val inputDF = vcf(input)
      .withColumn("file_name", regexp_extract(input_file_name(), ".*/(.*)", 1))
      .select(chromosome, start, end, reference, alternate, name, annotations)

    val consequences = Consequences.build(studyId, releaseId, inputDF)

    Consequences.write(consequences, studyId, releaseId, output)
  }
}
