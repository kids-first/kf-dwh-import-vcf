package org.kidsfirstdrc.dwh.updates

import bio.ferlab.datalake.spark3.public.SparkApp
import org.apache.spark.sql.SparkSession
import org.kidsfirstdrc.dwh.conf.Catalog.{Clinical, Public}

object Update extends SparkApp {

  val Array(_, source, destination) = args

  // calls SparkApp.init() to load configuration file passed as first argument as well as an instance of SparkSession
  implicit val (conf, spark) = init()

  run(source, destination)

  def run(source: String, destination: String)(implicit spark: SparkSession): Unit = {

    Seq("variant", "portal")
      .foreach { schema =>
        (source, destination) match {
          case ("clinvar", "variants") =>
            new UpdateClinical(Public.clinvar, Clinical.variants, schema).run()

          case ("topmed_bravo", "variants") =>
            new UpdateClinical(Public.topmed_bravo, Clinical.variants, schema).run()

          case ("gnomad_genomes_3_1_1", "variants") =>
            new UpdateClinical(Public.gnomad_genomes_3_1_1, Clinical.variants, schema).run()

          case ("dbnsfp_original", "consequences") =>
            new UpdateClinical(Public.dbnsfp_original, Clinical.consequences, schema).run()

          case _ => throw new IllegalArgumentException(s"No job found for : ($source, $destination)")
        }
      }

  }

}
