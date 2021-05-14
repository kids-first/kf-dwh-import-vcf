package org.kidsfirstdrc.dwh.conf

import bio.ferlab.datalake.spark3.config.{Configuration, SourceConf}

object SourceConfImplicit {

  implicit class SourceConfOps(ds: SourceConf) {
    def documentationPath(implicit conf: Configuration): String = {
      s"${ds.rootPath}/jobs/documentation/${ds.name}.json"
    }
  }
}
