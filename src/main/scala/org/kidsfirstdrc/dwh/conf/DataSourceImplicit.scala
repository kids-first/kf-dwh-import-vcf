package org.kidsfirstdrc.dwh.conf

import bio.ferlab.datalake.spark3.config.Configuration
import bio.ferlab.datalake.spark3.etl.DataSource

object DataSourceImplicit {

  implicit class DataSourceOps(ds: DataSource) {
    def documentationPath(implicit conf: Configuration): String = {
      s"${ds.rootPath}/jobs/documentation/${ds.name}.json"
    }
  }
}
