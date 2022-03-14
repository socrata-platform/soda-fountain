package com.socrata.soda.server.wiremodels

import com.socrata.soql.types._
import com.socrata.soda.external._
import com.rojoma.json.ast.JString

trait SoQLLiteralColumnCommonRep {
  def representedType: SoQLType
}

// We should NEVER have ReadReps.  If you need to read a soql literal,
// use the proper SoQL lexer.

trait SoQLLiteralColumnWriteRep extends SoQLLiteralColumnCommonRep {
  protected def toRawSoQLLiteral(input: SoQLValue): String
  final def toSoQLLiteral(input: SoQLValue): String = {
    val x = input match {
      case SoQLNull => "null"
      case notNull => "(" + toRawSoQLLiteral(notNull) + ")"
    }
    "(" + x + "::" + representedType.name.name + ")"
  }
}

trait SoQLLiteralColumnRep extends SoQLLiteralColumnWriteRep

object SoQLLiteralColumnRep {
  object TextRep extends SoQLLiteralColumnRep {
    val representedType = SoQLText
    def toRawSoQLLiteral(value: SoQLValue) =
      JString(value.asInstanceOf[SoQLText].value).toString // SoQL double-quoted strings have the same syntax as JSON strings
  }

  object NumberRep extends SoQLLiteralColumnRep {
    val representedType = SoQLNumber
    def toRawSoQLLiteral(value: SoQLValue) =
      value.asInstanceOf[SoQLNumber].value.toString
  }

  object MoneyRep extends SoQLLiteralColumnRep {
    val representedType = SoQLMoney
    def toRawSoQLLiteral(value: SoQLValue) =
      value.asInstanceOf[SoQLMoney].value.toString
  }

  object BooleanRep extends SoQLLiteralColumnRep {
    val representedType = SoQLBoolean
    def toRawSoQLLiteral(value: SoQLValue) =
      value.asInstanceOf[SoQLBoolean].value.toString
  }

  object IDRep extends SoQLLiteralColumnRep {
    val representedType = SoQLID
    val stringRep = JsonColumnRep.IdStringRep
    def toRawSoQLLiteral(value: SoQLValue) =
      JString(stringRep(value.asInstanceOf[SoQLID])).toString
  }

  object FixedTimestampRep extends SoQLLiteralColumnRep {
    val representedType = SoQLFixedTimestamp
    def toRawSoQLLiteral(value: SoQLValue) =
      JString(SoQLFixedTimestamp.StringRep(value.asInstanceOf[SoQLFixedTimestamp].value)).toString
  }

  object FloatingTimestampRep extends SoQLLiteralColumnRep {
    val representedType = SoQLFloatingTimestamp
    def toRawSoQLLiteral(value: SoQLValue) =
      JString(SoQLFloatingTimestamp.StringRep(value.asInstanceOf[SoQLFloatingTimestamp].value)).toString
  }

  object BlobRep extends SoQLLiteralColumnRep {
    val representedType = SoQLBlob
    def toRawSoQLLiteral(value: SoQLValue) =
      JString(value.asInstanceOf[SoQLBlob].value).toString
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
      SoQLBoolean -> BooleanRep,
      //SoQLObject -> ObjectRep,
      //SoQLArray -> ArrayRep,
      //SoQLJson -> JValueRep,
      SoQLBlob -> BlobRep
    )
}
