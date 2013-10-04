package com.socrata.soda.server.wiremodels

import com.socrata.soql.types._

/*
    The string column rep is used for row specifiers that are part of urls in single-row operations
 */

trait StringColumnCommonRep {
  def representedType: SoQLType
}

trait StringColumnReadRep extends StringColumnCommonRep {
  def fromString(input: String): Option[SoQLValue]
}

// If we ever generate links to particular rows, we'll want a
// StringColumnWriteRep.  But for now, YAGNI.

trait StringColumnRep extends StringColumnReadRep

object StringColumnRep {
  object TextRep extends StringColumnRep {
    val representedType = SoQLText
    def fromString(input: String) = Some(SoQLText(input))
  }

  object NumberRep extends StringColumnRep {
    val representedType = SoQLNumber
    def fromString(input: String) =
      try { Some(SoQLNumber(new java.math.BigDecimal(input))) }
      catch { case _: NumberFormatException => None }
  }

  object MoneyRep extends StringColumnRep {
    val representedType = SoQLMoney
    def fromString(input: String) =
      try { Some(SoQLMoney(new java.math.BigDecimal(input))) }
      catch { case _: NumberFormatException => None }
  }

  object BooleanRep extends StringColumnRep {
    val representedType = SoQLBoolean
    def fromString(input: String) = input.toLowerCase match {
      case "true" | "t" => Some(SoQLBoolean.canonicalTrue)
      case "false" | "f" => Some(SoQLBoolean.canonicalFalse)
      case _ => None
    }
  }

  object IDRep extends StringColumnRep {
    val representedType = SoQLID
    def fromString(input: String) = JsonColumnRep.IdStringRep.unapply(input)
  }

  object FixedTimestampRep extends StringColumnRep {
    val representedType = SoQLFixedTimestamp
    def fromString(input: String) = SoQLFixedTimestamp.StringRep.unapply(input).map(SoQLFixedTimestamp(_))
  }

  object FloatingTimestampRep extends StringColumnRep {
    val representedType = SoQLFloatingTimestamp
    def fromString(input: String) = SoQLFloatingTimestamp.StringRep.unapply(input).map(SoQLFloatingTimestamp(_))
  }

  val forType: Map[SoQLType, StringColumnRep] =
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
