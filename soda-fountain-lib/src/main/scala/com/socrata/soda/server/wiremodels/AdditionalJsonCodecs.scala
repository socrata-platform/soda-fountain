package com.socrata.soda.server.wiremodels

import com.rojoma.json.codec.JsonCodec
import com.socrata.soql.environment.{TypeName, ColumnName}
import com.rojoma.json.ast.{JString, JValue}
import com.socrata.soql.types.SoQLType

// JsonCodecs for things not under this project's immediate control.
object AdditionalJsonCodecs {
  implicit object ColumnNameCodec extends JsonCodec[ColumnName] {
    def encode(x: ColumnName): JValue = JString(x.name)

    def decode(x: JValue): Option[ColumnName] = x match {
      case JString(s) => Some(new ColumnName(s))
      case _ => None
    }
  }

  implicit object TypeNameCodec extends JsonCodec[TypeName] {
    def encode(x: TypeName): JValue = JString(x.name)

    def decode(x: JValue): Option[TypeName] = x match {
      case JString(s) => Some(new TypeName(s))
      case _ => None
    }
  }

  implicit object SoQLTypeCodec extends JsonCodec[SoQLType] {
    def encode(x: SoQLType): JValue = JString(x.name.name)

    def decode(x: JValue): Option[SoQLType] = x match {
      case JString(s) => SoQLType.typesByName.get(new TypeName(s))
      case _ => None
    }
  }
}
