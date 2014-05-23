package com.socrata.soda.server.wiremodels

import com.socrata.soql.types._
import com.rojoma.json.io.CompactJsonWriter

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

  object MoneyRep extends CsvColumnRep {
    def toString(value: SoQLValue) =
      if(SoQLNull == value) null
      else value.asInstanceOf[SoQLMoney].value.toString
  }

  object DoubleRep extends CsvColumnRep {
    def toString(value: SoQLValue) =
      if(SoQLNull == value) null
      else value.asInstanceOf[SoQLDouble].value.toString
  }

  object BooleanRep extends CsvColumnRep {
    def toString(value: SoQLValue) =
      if(SoQLNull == value) null
      else value.asInstanceOf[SoQLBoolean].value.toString
  }

  object FixedTimestampRep extends CsvColumnRep {
    def toString(value: SoQLValue) =
      if(SoQLNull == value) null
      else SoQLFixedTimestamp.StringRep(value.asInstanceOf[SoQLFixedTimestamp].value)
  }

  object FloatingTimestampRep extends CsvColumnRep {
    def toString(value: SoQLValue) =
      if(SoQLNull == value) null
      else SoQLFloatingTimestamp.StringRep(value.asInstanceOf[SoQLFloatingTimestamp].value)
  }

  object DateRep extends CsvColumnRep {
    def toString(value: SoQLValue) =
      if(SoQLNull == value) null
      else SoQLDate.StringRep(value.asInstanceOf[SoQLDate].value)
  }

  object TimeRep extends CsvColumnRep {
    def toString(value: SoQLValue) =
      if(SoQLNull == value) null
      else SoQLTime.StringRep(value.asInstanceOf[SoQLTime].value)
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

  object LocationRep extends CsvColumnRep {
    def toString(value: SoQLValue) =
      if (SoQLNull == value) null
      else {
        val loc = value.asInstanceOf[SoQLLocation]
        s"(${loc.latitude},${loc.longitude})"
      }
  }

  object ObjectRep extends CsvColumnRep {
    def toString(value: SoQLValue) =
      if(SoQLNull == value) null
      else CompactJsonWriter.toString(value.asInstanceOf[SoQLObject].value)
  }

  object ArrayRep extends CsvColumnRep {
    def toString(value: SoQLValue) =
      if(SoQLNull == value) null
      else CompactJsonWriter.toString(value.asInstanceOf[SoQLArray].value)
  }

  object JValueRep extends CsvColumnRep {
    def toString(value: SoQLValue) =
      if(SoQLNull == value) null
      else CompactJsonWriter.toString(value.asInstanceOf[SoQLJson].value)
  }

  object PointValueRep extends CsvColumnRep {
    def toString(value: SoQLValue) =
      if(SoQLNull == value) null
      else SoQLPoint.JsonRep(value.asInstanceOf[SoQLPoint].value)
  }

  object MultiLineValueRep extends CsvColumnRep {
    def toString(value: SoQLValue) =
      if(SoQLNull == value) null
      else SoQLMultiLine.JsonRep(value.asInstanceOf[SoQLMultiLine].value)
  }

  object MultiPolygonValueRep extends CsvColumnRep {
    def toString(value: SoQLValue) =
      if(SoQLNull == value) null
      else SoQLMultiPolygon.JsonRep(value.asInstanceOf[SoQLMultiPolygon].value)
  }

  val forType: Map[SoQLType, CsvColumnRep] = Map(
    SoQLText -> TextRep,
    SoQLFixedTimestamp -> FixedTimestampRep,
    SoQLFloatingTimestamp -> FloatingTimestampRep,
    SoQLDate -> DateRep,
    SoQLTime -> TimeRep,
    SoQLID -> IDRep,
    SoQLVersion -> VersionRep,
    SoQLNumber -> NumberRep,
    SoQLMoney -> MoneyRep,
    SoQLDouble -> DoubleRep,
    SoQLBoolean -> BooleanRep,
    SoQLLocation -> LocationRep,
    SoQLObject -> ObjectRep,
    SoQLArray -> ArrayRep,
    SoQLJson -> JValueRep,
    SoQLPoint -> PointValueRep,
    SoQLMultiLine -> MultiLineValueRep,
    SoQLMultiPolygon -> MultiPolygonValueRep
  )
}
