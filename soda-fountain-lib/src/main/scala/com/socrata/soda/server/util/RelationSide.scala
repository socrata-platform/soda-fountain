package com.socrata.soda.server.util

object RelationSide extends Enumeration {
  type RelationSide = Value

  val From = Value("from")
  val To = Value("to")
}
