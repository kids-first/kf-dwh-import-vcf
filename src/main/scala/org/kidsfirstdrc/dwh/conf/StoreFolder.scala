package org.kidsfirstdrc.dwh.conf

import bio.ferlab.datalake.spark3.config.SourceConf

trait StoreFolder {

  val alias: String

  private def getClassFields: Set[Any] =
    this
      .getClass
      .getDeclaredFields
      .foldLeft(List.empty[Any]) {
        case (acc, f) =>
          f.setAccessible(true)
          acc :+ f.get(this)
      }.toSet

  def sources: Set[SourceConf] = getClassFields.filter(_.isInstanceOf[SourceConf]).map(_.asInstanceOf[SourceConf])
}
