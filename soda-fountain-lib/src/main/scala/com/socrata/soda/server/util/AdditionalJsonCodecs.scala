package com.socrata.soda.server.util

import com.rojoma.json.codec.JsonCodec
import com.socrata.soql.environment.{TypeName, ColumnName}
import com.rojoma.json.ast.{JString, JValue}
import com.socrata.soql.types.SoQLType
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

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

  implicit object DateTimeCodec extends JsonCodec[DateTime] {
    val formatter = ISODateTimeFormat.dateTime
    val parser = ISODateTimeFormat.dateTimeParser
    def encode(x: DateTime): JValue = JString(formatter.print(x))
    def decode(x: JValue): Option[DateTime] = x match {
      case JString(s) =>
        try {
          Some(parser.parseDateTime(x.toString))
        } catch {
          case _: IllegalArgumentException => None
        }
      case _ =>
        None
    }
  }
}
