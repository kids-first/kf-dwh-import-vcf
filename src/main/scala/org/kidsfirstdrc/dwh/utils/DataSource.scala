package org.kidsfirstdrc.dwh.utils


case class DataSource(name: String,
                      database: String,
                      path: String,
                      format: Format,
                      dependsOn: List[DataSource] = List.empty[DataSource])
