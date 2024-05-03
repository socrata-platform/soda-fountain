package com.socrata.soda.server.id

import com.rojoma.json.v3.ast.{JNumber, JString, JValue}
import com.rojoma.json.v3.codec._
import com.rojoma.json.v3.codec.DecodeError.InvalidType
import com.rojoma.json.v3.codec.JsonDecode.DecodeResult
import com.rojoma.json.v3.codec.JsonEncode
import com.rojoma.json.v3.util.{WrapperJsonCodec, WrapperFieldCodec}

trait AbstractId {
  def underlying: String
}

case class DatasetInternalName(underlying: String) extends AbstractId {
  private val dotIndex = underlying.lastIndexOf('.')
  if(dotIndex == -1) {
    throw new IllegalArgumentException("Malformed internal name")
  }
  val datasetId = try {
    java.lang.Long.parseLong(underlying.substring(dotIndex + 1))
  } catch {
    case _ : NumberFormatException =>
      throw new IllegalArgumentException("Malformed internal name")
  }

  def nativeDataCoordinator = underlying.substring(0, dotIndex)
}

object DatasetInternalName {
  implicit val jCodec = WrapperJsonCodec[DatasetInternalName](DatasetInternalName(_), _.underlying)
  implicit val fCodec = WrapperFieldCodec[DatasetInternalName](DatasetInternalName(_), _.underlying)
}

case class ColumnId(underlying: String) extends AbstractId
object ColumnId {
  implicit val jCodec = WrapperJsonCodec[ColumnId](ColumnId(_), _.underlying)
  implicit val fCodec = WrapperFieldCodec[ColumnId](ColumnId(_), _.underlying)
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
      case n: JNumber => Right(RowSpecifier(n.toString))
      case u => Left(InvalidType(JString, u.jsonType))
    }}
}
