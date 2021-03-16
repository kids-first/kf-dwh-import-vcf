package org.kidsfirstdrc.dwh.conf

import bio.ferlab.datalake.core.etl.Format
import org.kidsfirstdrc.dwh.conf.Environment._

case class DataSource(name: String,
                      database: String,
                      bucket: String,
                      relativePath: String,
                      format: Format) {

  def path(implicit env: Environment): String = {
    env match {
      case PROD   => s"$bucket$relativePath"
      case QA     => s"$bucket/qa$relativePath"
      case DEV    => s"$bucket/dev$relativePath"
      case LOCAL  => s"""${this.getClass.getClassLoader.getResource(".").getFile + s"$relativePath"}"""
    }
  }

  def documentationPath(implicit env: Environment): String = {
    env match {
      case PROD   => s"$bucket/jobs/documentation/$name.json"
      case QA     => s"$bucket/qa/jobs/documentation/$name.json"
      case DEV    => s"$bucket/dev/jobs/documentation/$name.json"
      case LOCAL  => s"${this.getClass.getClassLoader.getResource("documentation").getFile}/$name.json"
    }
  }
}
