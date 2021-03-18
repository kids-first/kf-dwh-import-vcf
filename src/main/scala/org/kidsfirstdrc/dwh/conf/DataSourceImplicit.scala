package org.kidsfirstdrc.dwh.conf

import bio.ferlab.datalake.core.config.Configuration
import bio.ferlab.datalake.core.etl.DataSource

object DataSourceImplicit {

  implicit class DataSourceOps(ds: DataSource) {
    def documentationPath(implicit conf: Configuration): String = {
      s"${ds.rootPath}/jobs/documentation/${ds.name}.json"
    }

    def path(implicit env: Configuration): String = {
      ds.location
    }
  }
}
