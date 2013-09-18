package com.socrata.soda.server.wiremodels

import com.socrata.soql.types._

trait CsvColumnWriteRep {
  def toString(value: SoQLValue): String
}

abstract class CsvColumnRep extends CsvColumnWriteRep

object CsvColumnRep {

  object TextRep extends CsvColumnRep {
    def toString(value: SoQLValue) =
      if(SoQLNull == value) null
      else value.asInstanceOf[SoQLText].value
  }

  object NumberRep extends CsvColumnRep {
    def toString(value: SoQLValue) =
      if(SoQLNull == value) null
      else value.asInstanceOf[SoQLNumber].value.toString
  }

  object FixedTimestampRep extends CsvColumnRep {
    def toString(value: SoQLValue) =
      if(SoQLNull == value) null
      else SoQLFixedTimestamp.StringRep(value.asInstanceOf[SoQLFixedTimestamp].value)
  }

  object IDRep extends CsvColumnRep {
    def toString(value: SoQLValue) =
      if(SoQLNull == value) null
      else JsonColumnRep.IdStringRep(value.asInstanceOf[SoQLID])
  }

  object VersionRep extends CsvColumnRep {
    def toString(value: SoQLValue) =
      if(SoQLNull == value) null
      else JsonColumnRep.VersionStringRep(value.asInstanceOf[SoQLVersion])
  }

  def forType(typ: SoQLType): CsvColumnRep = typ match {
    case SoQLText => TextRep
    case SoQLNumber => NumberRep
    case SoQLFixedTimestamp => FixedTimestampRep
    case SoQLID => IDRep
    case SoQLVersion => VersionRep
  }
}
