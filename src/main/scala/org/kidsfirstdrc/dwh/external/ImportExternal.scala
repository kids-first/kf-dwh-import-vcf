package org.kidsfirstdrc.dwh.external

import bio.ferlab.datalake.core.config.{Configuration, StorageConf}
import org.apache.spark.sql.SparkSession
import org.kidsfirstdrc.dwh.conf.Catalog.Public
import org.kidsfirstdrc.dwh.external.clinvar.ImportClinVarJob
import org.kidsfirstdrc.dwh.external.dbnsfp.{ImportAnnovarScores, ImportRaw, ImportScores}
import org.kidsfirstdrc.dwh.external.omim.ImportOmimGeneSet
import org.kidsfirstdrc.dwh.external.orphanet.ImportOrphanetGeneSet
import org.kidsfirstdrc.dwh.updates.UpdateVariant

import scala.util.Try

object ImportExternal extends App {

  val Array(jobType, updateDependencies) = args

  implicit val spark: SparkSession = SparkSession.builder
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .enableHiveSupport()
    .appName("Import").getOrCreate()

  implicit val conf: Configuration = Configuration(List(StorageConf("kf-strides-variant", "s3a://kf-strides-variant-parquet-prd")))

  jobType.toLowerCase match {
    case "1000genomes"     => new Import1k().run()
    case "annovar_scores"  => new ImportAnnovarScores().run()
    case "cancer_hotspots" => new ImportCancerHotspots().run()
    case "clinvar"         =>
      new ImportClinVarJob().run()
      Try {
        if (updateDependencies.toBoolean)
          new UpdateVariant(Public.clinvar).run()
      }
    case "cosmic_gene_set" => new ImportCancerGeneCensus().run()
    case "ddd_gene_set"    => new ImportDDDGeneCensus().run()
    case "dbnsfp_variant"  => new ImportRaw().run()
    case "dbnsfp_original" => new ImportScores().run()
    case "dbsnp"           => new ImportDBSNP().run()
    case "genes"           => new ImportGenesTable().run()
    case "hpo_gene_set"    => new ImportHPOGeneSet().run()
    case "human_genes"     => new ImportHumanGenes().run()
    case "omim_gene_set"   => new ImportOmimGeneSet().run()
    case "orphanet"        => new ImportOrphanetGeneSet().run()
    case "topmed_bravo"    =>
      new ImportTopMed().run()
      Try {
        if (updateDependencies.toBoolean)
          new UpdateVariant(Public.topmed_bravo).run()
      }
  }

}
