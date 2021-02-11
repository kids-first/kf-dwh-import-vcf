package org.kidsfirstdrc.dwh.conf

object Environment extends Enumeration {
  type Environment = Value
  val PROD, QA, DEV, LOCAL = Value
}
