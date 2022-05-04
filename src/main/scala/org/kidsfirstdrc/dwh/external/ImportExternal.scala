package org.kidsfirstdrc.dwh.external

import bio.ferlab.datalake.spark3.public.SparkApp
import org.kidsfirstdrc.dwh.external.clinvar.ImportClinVarJob
import org.kidsfirstdrc.dwh.external.dbnsfp.{ImportAnnovarScores, ImportRaw, ImportScores}
import org.kidsfirstdrc.dwh.external.ensembl.ImportEnsemblMapping
import org.kidsfirstdrc.dwh.external.gnomad.ImportGnomadV311Job
import org.kidsfirstdrc.dwh.external.omim.ImportOmimGeneSet
import org.kidsfirstdrc.dwh.external.orphanet.ImportOrphanetGeneSet

object ImportExternal extends SparkApp {

  //args(0) -> configuration file path
  //args(1) -> run steps
  val Array(_, _, destination) = args

  // calls SparkApp.init() to load configuration file passed as first argument as well as an instance of SparkSession
  implicit val (conf, runSteps, spark) = init()

  log.info(s"Loading $conf")
  log.info(s"${conf.sources.mkString}")

  destination.toLowerCase match {
    case "1000_genomes"         => new Import1k().run()
    case "annovar_scores"       => new ImportAnnovarScores().run()
    case "cancer_hotspots"      => new ImportCancerHotspots().run()
    case "clinvar"              => new ImportClinVarJob().run()
    case "cosmic_gene_set"      => new ImportCancerGeneCensus().run()
    case "ddd_gene_set"         => new ImportDDDGeneCensus().run()
    case "dbnsfp_variant"       => new ImportRaw().run()
    case "dbnsfp_original"      => new ImportScores().run()
    case "dbsnp"                => new ImportDBSNP().run()
    case "genes"                => new ImportGenesTable().run()
    case "gnomad_genomes_3_1_1" => new ImportGnomadV311Job().run()
    case "ensembl_mapping"      => new ImportEnsemblMapping().run()
    case "hpo_gene_set"         => new ImportHPOGeneSet().run()
    case "human_genes"          => new ImportHumanGenes().run()
    case "omim_gene_set"        => new ImportOmimGeneSet().run()
    case "orphanet_gene_set"    => new ImportOrphanetGeneSet().run()
    case "topmed_bravo"         => new ImportTopMed().run()
  }

}
