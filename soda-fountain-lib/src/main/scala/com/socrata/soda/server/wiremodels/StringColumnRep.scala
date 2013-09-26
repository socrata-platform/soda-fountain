package com.socrata.soda.server.wiremodels

import com.socrata.soql.types._
import com.rojoma.json.io.CompactJsonWriter
import com.rojoma.json.ast.{JBoolean, JString, JValue}
import java.math.BigDecimal

/*
    The string column rep is used for row specifiers that are part of urls in single-row operations
 */

trait StringColumnReadRep {
  def fromString(input: String): Option[SoQLValue]
  def toSoQLLiteral(value: SoQLValue): String
  def toJValue(value: SoQLValue): JValue
}

trait StringColumnRep extends StringColumnReadRep

object StringColumnRep {

  object TextRep extends StringColumnRep {
    def fromString(input: String) = Some(SoQLText(input))
    def toSoQLLiteral(value: SoQLValue) =
      if(SoQLNull == value) "null"
      else s"'${value.asInstanceOf[SoQLText].value}'"
    def toJValue(value: SoQLValue) = JString(value.asInstanceOf[SoQLText].value)
  }

  object NumberRep extends StringColumnRep {
    def fromString(input: String) = Some(SoQLNumber(new BigDecimal(input)))
    def toSoQLLiteral(value: SoQLValue) =
      if(SoQLNull == value) "null"
      else value.asInstanceOf[SoQLNumber].value.toString
    def toJValue(value: SoQLValue) = JString(value.asInstanceOf[SoQLNumber].value.toString)
  }

  object BooleanRep extends StringColumnRep {
    def fromString(input: String) = input.toLowerCase match {
      case "true" => Some(SoQLBoolean(true))
      case "t" => Some(SoQLBoolean(true))
      case "false" => Some(SoQLBoolean(false))
      case "f" => Some(SoQLBoolean(false))
      case _ => None
    }
    def toSoQLLiteral(value: SoQLValue) =
      if(SoQLNull == value) "null"
      else value.asInstanceOf[SoQLBoolean].value.toString
    def toJValue(value: SoQLValue) = JBoolean(value.asInstanceOf[SoQLBoolean].value)
  }

  val forSoQLLiteral: Map[SoQLType, StringColumnRep] =
    Map(
      SoQLText -> TextRep,
      //SoQLFixedTimestamp -> FixedTimestampRep,
      //SoQLFloatingTimestamp -> FloatingTimestampRep,
      //SoQLDate -> DateRep,
      //SoQLTime -> TimeRep,
      //SoQLID -> IDRep,
      //SoQLVersion -> VersionRep,
      SoQLNumber -> NumberRep,
      //SoQLMoney -> MoneyRep,
      //SoQLDouble -> DoubleRep,
      SoQLBoolean -> BooleanRep
      //SoQLObject -> ObjectRep,
      //SoQLArray -> ArrayRep,
      //SoQLJson -> JValueRep
    )
}
