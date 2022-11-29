package org.kidsfirstdrc.dwh.updates

import bio.ferlab.datalake.spark3.public.SparkApp
import org.apache.spark.sql.SparkSession
import org.kidsfirstdrc.dwh.conf.Catalog.{Clinical, Public}
import org.kidsfirstdrc.dwh.external.ImportExternal.{args, init}

object Update extends SparkApp {

  //args(0) -> configuration file path
  //args(1) -> run steps
  val Array(_, _, source, destination) = args

  // calls SparkApp.init() to load configuration file passed as first argument as well as an instance of SparkSession
  implicit val (conf, runSteps, spark) = init()

  run(source, destination)

  def run(source: String, destination: String)(implicit spark: SparkSession): Unit = {

    Seq("variant", "portal")
      .foreach { schema =>
        (source, destination) match {
          case ("clinvar", "variants") =>
            new UpdateClinical(Public.clinvar, Clinical.variants, schema).run()

          case ("1000_genomes", "variants") =>
            new UpdateClinical(Public.`1000_genomes`, Clinical.variants, schema).run()

          case ("topmed_bravo", "variants") =>
            new UpdateClinical(Public.topmed_bravo, Clinical.variants, schema).run()

          case ("gnomad_exomes_2_1", "variants") =>
            new UpdateClinical(Public.gnomad_exomes_2_1, Clinical.variants, schema).run()

          case ("gnomad_genomes_2_1", "variants") =>
            new UpdateClinical(Public.gnomad_genomes_2_1, Clinical.variants, schema).run()

          case ("gnomad_genomes_3_0", "variants") =>
            new UpdateClinical(Public.gnomad_genomes_3_0, Clinical.variants, schema).run()

          case ("gnomad_genomes_3_1_1", "variants") =>
            new UpdateClinical(Public.gnomad_genomes_3_1_1, Clinical.variants, schema).run()

          case ("dbnsfp_original", "consequences") =>
            new UpdateClinical(Public.dbnsfp_original, Clinical.consequences, schema).run()

          case _ => throw new IllegalArgumentException(s"No job found for : ($source, $destination)")
        }
      }

  }

}
