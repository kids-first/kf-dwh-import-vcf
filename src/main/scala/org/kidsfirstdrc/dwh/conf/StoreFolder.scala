package org.kidsfirstdrc.dwh.conf

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


  def sources: Set[DataSource] = getClassFields.filter(_.isInstanceOf[DataSource]).map(_.asInstanceOf[DataSource])
}
