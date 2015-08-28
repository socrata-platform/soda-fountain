package com.socrata.soda.server.id

import com.rojoma.json.v3.ast.{JString, JValue}
import com.rojoma.json.v3.codec._
import com.rojoma.json.v3.codec.DecodeError.InvalidType
import com.rojoma.json.v3.codec.JsonDecode.DecodeResult
import com.rojoma.json.v3.codec.JsonEncode

trait AbstractId {
  def underlying: String
}

case class DatasetId(underlying: String) extends AbstractId {
  def nativeDataCoordinator = underlying.substring(0, underlying.lastIndexOf('.'))
}

object DatasetId {
  implicit val jCodec = new JsonEncode[DatasetId] with JsonDecode[DatasetId] {
    def encode(x: DatasetId): JValue = JString(x.underlying)
    def decode(x: JValue): DecodeResult[DatasetId] = x match {
      case JString(n) => Right(DatasetId(n))
      case u => Left(InvalidType(JString, u.jsonType))
    }
  }
}

case class ColumnId(underlying: String) extends AbstractId
object ColumnId {
  implicit val jCodec = new JsonEncode[ColumnId] with JsonDecode[ColumnId] {
    def encode(x: ColumnId): JValue = JString(x.underlying)

    def decode(x: JValue): DecodeResult[ColumnId] = x match {
      case JString(n) => Right(ColumnId(n))
      case u => Left(InvalidType(JString, u.jsonType))
    }
  }

  implicit val ordering = new Ordering[ColumnId] {
    def compare(x: ColumnId, y: ColumnId): Int = x.underlying.compareTo(y.underlying)
  }
}

case class SecondaryId(underlying: String) extends AbstractId

case class RowSpecifier(underlying: String) extends AbstractId

object RowSpecifier {
  implicit val jCodec = new JsonEncode[RowSpecifier] with JsonDecode[RowSpecifier] {
    def encode(x: RowSpecifier): JValue = JString(x.underlying)
    def decode(x: JValue): DecodeResult[RowSpecifier] = x match {
      case JString(s) => Right(RowSpecifier(s))
      case u => Left(InvalidType(JString, u.jsonType))
    }}
}
