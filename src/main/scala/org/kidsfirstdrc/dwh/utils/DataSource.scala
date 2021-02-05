package org.kidsfirstdrc.dwh.utils

import org.kidsfirstdrc.dwh.utils.Environment._


case class DataSource(name: String,
                      database: String,
                      bucket: String,
                      relativePath: String,
                      format: Format,
                      dependsOn: List[DataSource] = List.empty[DataSource]) {

  def path(implicit env: Environment): String = {
    env match {
      case PROD   => s"$bucket$relativePath"
      case QA     => s"$bucket/qa$relativePath"
      case DEV    => s"$bucket/dev$relativePath"
      case LOCAL  => s"${this.getClass.getResource(relativePath)}"
    }
  }

  def documentationPath(implicit env: Environment): String = {
    env match {
      case PROD   => s"$bucket/jobs/documentation/$name.json"
      case QA     => s"$bucket/qa/jobs/documentation/$name.json"
      case DEV    => s"$bucket/dev/jobs/documentation/$name.json"
      case LOCAL  => s"${this.getClass.getResource("/documentation")}/$name.json"
    }
  }
}