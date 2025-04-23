package com.socrata.soda.server.wiremodels

import com.rojoma.json.v3.ast.{JBoolean, JString, JValue}
import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode, DecodeError}
import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, AllowMissing}

import com.socrata.soql.analyzer2.UnparsedFoundTables
import com.socrata.soql.analyzer2
import com.socrata.soql.analyzer2.rewrite.Pass
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types.{SoQLType, SoQLValue}
import com.socrata.soql.stdlib.analyzer2.Context
import com.socrata.soql.sql.Debug

import com.socrata.soda.server.copy.Stage
import com.socrata.soda.server.id.ResourceName

sealed abstract class PreserveSystemColumns
object PreserveSystemColumns {
  case object Never extends PreserveSystemColumns
  case object NonAggregated extends PreserveSystemColumns
  case object Always extends PreserveSystemColumns

  implicit object jCodec extends JsonEncode[PreserveSystemColumns] with JsonDecode[PreserveSystemColumns] {
    def encode(psc: PreserveSystemColumns) =
      psc match {
        case Never => JBoolean(false)
        case NonAggregated => JString("non_aggregated")
        case Always => JBoolean(true)
      }
    def decode(x: JValue) =
      x match {
        case JBoolean(false) | JString("never") => Right(Never)
        case JBoolean(true) | JString("always") => Right(Always)
        case JString("non_aggregated") => Right(NonAggregated)
        case other@JString(_) => Left(DecodeError.InvalidValue(other))
        case other => Left(DecodeError.join(
                             List(
                               DecodeError.InvalidType(expected = JString, got = other.jsonType),
                               DecodeError.InvalidType(expected = JBoolean, got = other.jsonType)
                             )))
      }
  }
}

case class FoundTablesRequest(
  tables: UnparsedFoundTables[metatypes.QueryMetaTypes],
  @AllowMissing("Context.empty")
  context: Context,
  @AllowMissing("Nil")
  rewritePasses: Seq[Seq[Pass]],
  @AllowMissing("true")
  allowRollups: Boolean,
  @AllowMissing("PreserveSystemColumns.Never")
  preserveSystemColumns: PreserveSystemColumns,
  debug: Option[Debug],
  queryTimeoutMS: Option[Long],
  store: Option[String]
)

object FoundTablesRequest {
  implicit val codec = AutomaticJsonCodecBuilder[FoundTablesRequest]
}
