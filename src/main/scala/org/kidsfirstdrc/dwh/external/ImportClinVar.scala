package org.kidsfirstdrc.dwh.external

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.kidsfirstdrc.dwh.glue.SetGlueTableComments
import org.kidsfirstdrc.dwh.updates.UpdateVariant
import org.kidsfirstdrc.dwh.utils.{Environment, EtlJob}
import org.kidsfirstdrc.dwh.utils.SparkUtils._
import org.kidsfirstdrc.dwh.utils.SparkUtils.columns._

import scala.collection.mutable
import scala.util.Try

object ImportClinVar extends App with EtlJob {

  override val database = "variant"
  override val tableName = "clinvar"

  val Array(clinvarDate, update_dependencies) = args
  println(s"""arguments ${args.mkString("[", ", ", "]")}""")

  implicit val spark: SparkSession = SparkSession.builder
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .appName("Import ClinVar").getOrCreate()

  val input = s"s3a://kf-strides-variant-parquet-prd/raw/clinvar/clinvar_${clinvarDate.trim}.vcf.gz"
  val output = "s3a://kf-strides-variant-parquet-prd/public"

  val sourceDF = extract(input)
  val resultDF = transform(sourceDF)
  load(resultDF, output)

  Try {
    if(update_dependencies.toBoolean)
      new UpdateVariant(Environment.PROD).run("s3a://kf-strides-variant-parquet-prd", "s3a://kf-strides-variant-parquet-prd")
  }


  override def extract(input: String)(implicit spark: SparkSession): DataFrame = {
    vcf(input)
  }

  override def transform(data: DataFrame)(implicit spark: SparkSession): DataFrame = {
    spark.udf.register("allele_origin", allele_origin_udf)

    val intermediateDf =
      data
      .select(
        chromosome +:
          start +:
          end +:
          name +:
          reference +:
          alternate +:
          (col("INFO_CLNSIG") as "clin_sig") +:
          (col("INFO_CLNSIGCONF") as "clin_sig_conflict") +:
          info_fields(data, "INFO_CLNSIG", "INFO_CLNSIGCONF"): _*)
        .withColumn("clin_sig", split(regexp_replace(concat_ws("|", col("clin_sig")), "^_|\\|_|/", "|"), "\\|"))
        .withColumn("clnrevstat", split(regexp_replace(concat_ws("|", col("clnrevstat")), "^_|\\|_|/", "|"), "\\|"))
        .withColumn("clin_sig_conflict", split(regexp_replace(concat_ws("|", col("clin_sig_conflict")), "\\(\\d{1,2}\\)", ""), "\\|"))

    intermediateDf
      .withInterpretations
      .withColumn("clndisdb", split(concat_ws("|", col("clndisdb")), "\\|"))
      .withColumn("clndn", split(concat_ws("", col("clndn")), "\\|"))
      .withColumn("conditions", split(regexp_replace(concat_ws("|", col("clndn")), "_", " "), "\\|"))
      .withColumn("clndisdbincl", split(concat_ws("", col("clndisdbincl")), "\\|"))
      .withColumn("clndnincl", split(concat_ws("", col("clndnincl")), "\\|"))
      .withColumn("mc", split(concat_ws("", col("mc")), "\\|"))
      .withColumn("allele_origin", allele_origin_udf(col("origin")))
      .drop("clin_sig_original", "clndn")

  }

  override def load(data: DataFrame, output: String)(implicit spark: SparkSession): Unit = {
    data.coalesce(1)
      .write
      .mode("overwrite")
      .format("parquet")
      .option("path", s"$output/$tableName")
      .saveAsTable(s"$database.$tableName")

    val metadata_file = s"s3a://kf-strides-variant-parquet-prd/jobs/documentation/$tableName.json"
    SetGlueTableComments.run(database, tableName, metadata_file)

    spark.sql(s"create or replace view variant_live.$tableName as select * from $database.$tableName")
  }

  def info_fields(df: DataFrame, excludes: String*): Seq[Column] = {
    df.columns.collect { case c if c.startsWith("INFO") && !excludes.contains(c) => col(c) as c.replace("INFO_", "").toLowerCase }
  }

  def info_fields_names(df: DataFrame, excludes: String*): Seq[String] = {
    df.columns.collect { case c if c.startsWith("INFO") && !excludes.contains(c) => c.replace("INFO_", "").toLowerCase }
  }

  def allele_origin_udf: UserDefinedFunction = udf { array: mutable.WrappedArray[String] =>

    val unknown = "unknown"

    val labels = Map(
      0 -> unknown,
      1 -> "germline",
      2 -> "somatic",
      4 -> "inherited",
      8 -> "paternal",
      16 -> "maternal",
      32 -> "de-novo",
      64 -> "biparental",
      128 -> "uniparental",
      256 -> "not-tested",
      512 -> "tested-inconclusive",
      1073741824 -> "other"
    )

    array match {
      //in case the inpout column is null return an Array("unknown")
      case null =>
        val wa: mutable.WrappedArray[String] = mutable.WrappedArray.make(Array(unknown))
        wa
      //in any other case transform the array of number into their labels
      case _ =>
        array
          .map(_.toInt)
          .flatMap {number =>
            labels.collect {
              case (_, _) if number == 0 => unknown
              case (digit, str) if (number & digit) != 0 => str
            }
          }.distinct
    }
  }

  implicit class DataFrameOps(df: DataFrame) {
    def withInterpretations: DataFrame = {

      val fieldsToRemove: Seq[String] = Seq("chromosome", "start", "end", "reference", "alternate", "interpretation")

      val fieldsTokeep: Seq[String] = df.columns.filterNot(c => fieldsToRemove.contains(c))
      df.withColumn("interpretations", array_union(col("clin_sig"), col("clin_sig_conflict")))
        .withColumn("interpretation", explode(col("interpretations")))
        .drop("interpretations")
        .groupBy("chromosome", "start", "end", "reference", "alternate")
        .agg(collect_set(col("interpretation")) as "interpretations", fieldsTokeep.map(c => first(c) as c):_*)
    }
  }
}
