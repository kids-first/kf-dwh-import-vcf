package org.kidsfirstdrc.dwh.external

import org.apache.spark.sql.SparkSession
import org.kidsfirstdrc.dwh.conf.Catalog.Public
import org.kidsfirstdrc.dwh.conf.Environment
import org.kidsfirstdrc.dwh.external.clinvar.ImportClinVarJob
import org.kidsfirstdrc.dwh.external.dbnsfp.ImportAnnovarScores
import org.kidsfirstdrc.dwh.external.omim.ImportOmimGeneSet
import org.kidsfirstdrc.dwh.external.orphanet.ImportOrphanetJob
import org.kidsfirstdrc.dwh.updates.UpdateVariant

import scala.util.Try

object ImportExternal extends App {

  val Array(jobType, runEnv, updateDependencies) = args

  implicit val spark: SparkSession = SparkSession.builder
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .appName("Import").getOrCreate()

  val env = Try(Environment.withName(runEnv)).getOrElse(Environment.DEV)

  jobType.toLowerCase match {
    case "clinvar"         =>
      new ImportClinVarJob(env).run()
      Try {
        if (updateDependencies.toBoolean)
          new UpdateVariant(Public.clinvar, env).run()
      }
    case "cosmic_gene_set" => new ImportCancerGeneCensus(env).run()
    case "ddd_gene_set"    => new ImportDDDGeneCensus(env).run()
    case "omim_gene_set"   => new ImportOmimGeneSet(env).run()
    case "orphanet"        => new ImportOrphanetJob(env).run()
    case "annovar_scores"  => new ImportAnnovarScores(env).run()
    case "topmed_bravo"    =>
      new ImportTopMed(env).run()
      Try {
        if (updateDependencies.toBoolean)
          new UpdateVariant(Public.topmed_bravo, env).run()
      }
  }

}
