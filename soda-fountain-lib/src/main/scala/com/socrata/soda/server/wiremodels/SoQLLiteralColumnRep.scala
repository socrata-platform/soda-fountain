package com.socrata.soda.server.wiremodels

import com.socrata.soql.types._
import com.rojoma.json.ast.JString

trait SoQLLiteralColumnCommonRep {
  def representedType: SoQLType
}

// We should NEVER have ReadReps.  If you need to read a soql literal,
// use the proper SoQL lexer.

trait SoQLLiteralColumnWriteRep extends SoQLLiteralColumnCommonRep {
  def toSoQLLiteral(input: SoQLValue): String
}

trait SoQLLiteralColumnRep extends SoQLLiteralColumnWriteRep

object SoQLLiteralColumnRep {
  object TextRep extends SoQLLiteralColumnRep {
    val representedType = SoQLText
    def toSoQLLiteral(value: SoQLValue) =
      if(SoQLNull == value) "null"
      else JString(value.asInstanceOf[SoQLText].value).toString // SoQL double-quoted strings have the same syntax as JSON strings
  }

  object NumberRep extends SoQLLiteralColumnRep {
    val representedType = SoQLNumber
    def toSoQLLiteral(value: SoQLValue) =
      if(SoQLNull == value) "null"
      else value.asInstanceOf[SoQLNumber].value.toString
  }

  object MoneyRep extends SoQLLiteralColumnRep {
    val representedType = SoQLMoney
    def toSoQLLiteral(value: SoQLValue) =
      if(SoQLNull == value) "null"
      else "(" + value.asInstanceOf[SoQLMoney].value.toString + "::money)"
  }

  object BooleanRep extends SoQLLiteralColumnRep {
    val representedType = SoQLBoolean
    def toSoQLLiteral(value: SoQLValue) =
      if(SoQLNull == value) "null"
      else value.asInstanceOf[SoQLBoolean].value.toString
  }

  object IDRep extends SoQLLiteralColumnRep {
    val representedType = SoQLID
    val stringRep = JsonColumnRep.IdStringRep
    def toSoQLLiteral(value: SoQLValue) =
      if(SoQLNull == value) "null"
      else "(" + JString(stringRep(value.asInstanceOf[SoQLID])).toString + "::row_identifier)"
  }

  object FixedTimestampRep extends SoQLLiteralColumnRep {
    val representedType = SoQLFixedTimestamp
    def toSoQLLiteral(value: SoQLValue) =
      if(SoQLNull == value) "null"
      else "(" + JString(SoQLFixedTimestamp.StringRep(value.asInstanceOf[SoQLFixedTimestamp].value)).toString + "::fixed_timestamp)"
  }

  object FloatingTimestampRep extends SoQLLiteralColumnRep {
    val representedType = SoQLFloatingTimestamp
    def toSoQLLiteral(value: SoQLValue) =
      if(SoQLNull == value) "null"
      else "(" + JString(SoQLFloatingTimestamp.StringRep(value.asInstanceOf[SoQLFloatingTimestamp].value)).toString + "::floating_timestamp)"
  }

  val forType: Map[SoQLType, SoQLLiteralColumnRep] =
    Map(
      SoQLText -> TextRep,
      SoQLFixedTimestamp -> FixedTimestampRep,
      SoQLFloatingTimestamp -> FloatingTimestampRep,
      //SoQLDate -> DateRep,
      //SoQLTime -> TimeRep,
      SoQLID -> IDRep,
      //SoQLVersion -> VersionRep,
      SoQLNumber -> NumberRep,
      SoQLMoney -> MoneyRep,
      //SoQLDouble -> DoubleRep,
      SoQLBoolean -> BooleanRep
      //SoQLObject -> ObjectRep,
      //SoQLArray -> ArrayRep,
      //SoQLJson -> JValueRep
    )
}
