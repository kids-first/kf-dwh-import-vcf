package org.kidsfirstdrc.dwh.conf

import bio.ferlab.datalake.core.config.Configuration
import bio.ferlab.datalake.core.etl.DataSource
import bio.ferlab.datalake.core.loader.Format
import org.kidsfirstdrc.dwh.conf.Environment._

case class Ds(name: String,
              database: String,
              bucket: String,
              relativePath: String,
              format: Format)

object Ds {
  implicit class DsOps(ds: Ds) {
    def documentationPath(implicit env: Environment): String = {
      env match {
        case PROD   => s"${ds.bucket}/jobs/documentation/${ds.name}.json"
        case QA     => s"${ds.bucket}/qa/jobs/documentation/${ds.name}.json"
        case DEV    => s"${ds.bucket}/dev/jobs/documentation/${ds.name}.json"
        case LOCAL  => s"${this.getClass.getClassLoader.getResource("documentation").getFile}/${ds.name}.json"
      }
    }

    def path(implicit env: Environment): String = {
      env match {
        case PROD   => s"${ds.bucket}${ds.relativePath}"
        case QA     => s"${ds.bucket}/qa${ds.relativePath}"
        case DEV    => s"${ds.bucket}/dev${ds.relativePath}"
        case LOCAL  => s"""${this.getClass.getClassLoader.getResource(".").getFile + s"${ds.relativePath}"}"""
      }
    }
  }

  implicit class DataSourceOps(ds: DataSource) {
    def documentationPath(implicit conf: Configuration): String = {
      s"${ds.rootPath}/jobs/documentation/${ds.name}.json"
    }

    def path(implicit env: Configuration): String = {
      ds.location
    }
  }
}
