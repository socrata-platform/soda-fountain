package com.socrata.soda.server.util.schema

import com.rojoma.json.v3.ast.{JObject, JString, JValue}
import com.rojoma.json.v3.codec.{JsonDecode, JsonEncode}
import com.rojoma.json.v3.codec.DecodeError.InvalidValue
import com.rojoma.json.v3.codec.JsonDecode.DecodeResult
import com.rojoma.json.v3.util.AutomaticJsonCodecBuilder
import com.socrata.soda.server.id.ColumnId
import com.socrata.soql.environment.TypeName
import com.socrata.soql.types.SoQLType

case class SchemaSpec(hash: String, locale: String, pk: ColumnId, schema: Map[ColumnId, SoQLType])
object SchemaSpec {
  private implicit val anotherMapCodec = new JsonEncode[Map[ColumnId, SoQLType]] with JsonDecode[Map[ColumnId, SoQLType]] {
    def encode(x: Map[ColumnId, SoQLType]) = JObject(x.map(pair => (pair._1.underlying, JString(pair._2.name.name))))
    def decode(x: JValue): DecodeResult[Map[ColumnId, SoQLType]] = x match {
      case JObject(m) =>
        val result = Map.newBuilder[ColumnId, SoQLType]
        m foreach {
          case (col, JString(typname)) =>
            result += new ColumnId(col) -> SoQLType.typesByName.get(TypeName(typname)).getOrElse {
              return Left(InvalidValue(x))
            }
          case _ =>
            return Left(InvalidValue(x))
        }
        Right(result.result())
      case _ =>
        Left(InvalidValue(x))
    }
  }

  implicit val codec = AutomaticJsonCodecBuilder[SchemaSpec]
}
