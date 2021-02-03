package org.kidsfirstdrc.dwh.utils

import org.kidsfirstdrc.dwh.utils.Environment._


case class DataSource(name: String,
                      database: String,
                      bucket: String,
                      relativepath: String,
                      format: Format,
                      dependsOn: List[DataSource] = List.empty[DataSource]) {

  def path(implicit env: Environment): String = {
    env match {
      case PROD   => s"$bucket$relativepath"
      case QA     => s"$bucket/qa$relativepath"
      case DEV    => s"$bucket/dev$relativepath"
      case LOCAL  => s"${this.getClass.getResource(relativepath)}"
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