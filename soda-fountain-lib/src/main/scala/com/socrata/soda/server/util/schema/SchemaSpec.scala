package com.socrata.soda.server.util.schema

import com.socrata.soda.server.id.ColumnId
import com.socrata.soql.types.SoQLType
import com.rojoma.json.util.AutomaticJsonCodecBuilder
import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.ast.{JValue, JString, JObject}
import com.socrata.soql.environment.TypeName

case class SchemaSpec(hash: String, locale: String, pk: ColumnId, schema: Map[ColumnId, SoQLType])
object SchemaSpec {
  private implicit val anotherMapCodec = new JsonCodec[Map[ColumnId, SoQLType]] {
    def encode(x: Map[ColumnId, SoQLType]) = JObject(x.map(pair => (pair._1.underlying, JString(pair._2.name.name))))
    def decode(x: JValue): Option[Map[ColumnId, SoQLType]] = x match {
      case JObject(m) =>
        val result = Map.newBuilder[ColumnId, SoQLType]
        m foreach {
          case (col, JString(typname)) =>
            result += new ColumnId(col) -> SoQLType.typesByName.get(TypeName(typname)).getOrElse { return None }
          case _ =>
            return None
        }
        Some(result.result())
      case _ =>
        None
    }
  }

  implicit val codec = AutomaticJsonCodecBuilder[SchemaSpec]
}
