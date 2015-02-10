package com.socrata.soda.server.util

import com.rojoma.json.v3.ast.{JString, JValue}
import com.rojoma.json.v3.codec.DecodeError.{InvalidValue, InvalidType}
import com.rojoma.json.v3.codec.JsonDecode._
import com.rojoma.json.v3.codec.{JsonDecode, JsonEncode}
import com.socrata.soql.environment.{TypeName, ColumnName}
import com.socrata.soql.types.SoQLType
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

// JsonCodecs for things not under this project's immediate control.
object AdditionalJsonCodecs {
  implicit object ColumnNameCodec extends JsonEncode[ColumnName] with JsonDecode[ColumnName] {
    def encode(x: ColumnName): JValue = JString(x.name)

    def decode(x: JValue): DecodeResult[ColumnName] = x match {
      case JString(s) => Right(new ColumnName(s))
      case u => Left(InvalidType(JString, u.jsonType))
    }
  }

  implicit object TypeNameCodec extends JsonEncode[TypeName] with JsonDecode[TypeName] {
    def encode(x: TypeName): JValue = JString(x.name)

    def decode(x: JValue): DecodeResult[TypeName] = x match {
      case JString(s) => Right(new TypeName(s))
      case u => Left(InvalidType(JString, u.jsonType))
    }
  }

  implicit object SoQLTypeCodec extends JsonEncode[SoQLType] with JsonDecode[SoQLType] {
    def encode(x: SoQLType): JValue = JString(x.name.name)

    def decode(x: JValue): DecodeResult[SoQLType] = x match {
      case JString(s) => Right(SoQLType.typesByName(new TypeName(s)))
      case u => Left(InvalidType(JString, u.jsonType))
    }
  }
}
