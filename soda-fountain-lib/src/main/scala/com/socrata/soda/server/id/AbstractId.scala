package com.socrata.soda.server.id

import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.ast.{JString, JValue}

trait AbstractId {
  def underlying: String
}

case class DatasetId(underlying: String) extends AbstractId {
  def nativeDataCoordinator = underlying.substring(0, underlying.lastIndexOf('.'))
}

case class ColumnId(underlying: String) extends AbstractId
object ColumnId {
  implicit val jCodec = new JsonCodec[ColumnId] {
    def encode(x: ColumnId): JValue = JString(x.underlying)

    def decode(x: JValue): Option[ColumnId] = x match {
      case JString(n) => Some(ColumnId(n))
      case _ => None
    }
  }
}

case class SecondaryId(underlying: String) extends AbstractId
