package org.kidsfirstdrc.dwh.vcf.util

import java.io.File

import org.apache.spark.sql.SparkSession

trait WithSparkSession {

  private val tmp = new File("tmp").getAbsolutePath
  implicit lazy val spark: SparkSession = SparkSession.builder()
    .config("spark.ui.enabled", value = false)
    .config("spark.sql.warehouse.dir", s"$tmp/wharehouse")
    .config("spark.driver.extraJavaOptions", s"-Dderby.system.home=$tmp/derby")
    .enableHiveSupport()
    .master("local")
    .getOrCreate()
}
