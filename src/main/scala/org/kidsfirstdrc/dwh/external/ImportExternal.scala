package org.kidsfirstdrc.dwh.external

import org.apache.spark.sql.SparkSession
import org.kidsfirstdrc.dwh.external.clinvar.ImportClinVarJob
import org.kidsfirstdrc.dwh.external.omim.ImportOmimGeneSet
import org.kidsfirstdrc.dwh.external.orphanet.ImportOrphanetJob
import org.kidsfirstdrc.dwh.updates.UpdateVariant
import org.kidsfirstdrc.dwh.utils.Environment

import scala.util.Try

object ImportExternal extends App {

  val Array(jobType, runEnv, updateDependencies) = args

  implicit val spark: SparkSession = SparkSession.builder
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .appName("Import").getOrCreate()

  val env = Try(Environment.withName(runEnv)).getOrElse(Environment.DEV)

  jobType.toLowerCase match {
    case "clinvar"  =>
      new ImportClinVarJob(env).run()
      Try {
        if (updateDependencies.toBoolean)
          new UpdateVariant(env)
            .run("s3a://kf-strides-variant-parquet-prd", "s3a://kf-strides-variant-parquet-prd")
      }
    case "omim"     => new ImportOmimGeneSet(env).run()
    case "orphanet" => new ImportOrphanetJob(env).run()
  }

}
