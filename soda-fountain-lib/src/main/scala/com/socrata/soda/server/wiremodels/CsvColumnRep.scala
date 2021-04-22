package com.socrata.soda.server.wiremodels

import java.net.URLEncoder

import com.rojoma.json.v3.ast.{JObject, JString}
import com.rojoma.json.v3.codec.{JsonDecode, JsonEncode}
import com.rojoma.json.v3.io.CompactJsonWriter
import com.rojoma.json.v3.util.JsonUtil
import com.socrata.soql.types._
import com.vividsolutions.jts.geom._

/**
  * EN-19815 - Rethink what we want to do about export format (csv) in Soda Fountain. Maybe it should be removed.
  */
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

  object ClearIDRep extends CsvColumnRep {
    def toString(value: SoQLValue) =
      if(SoQLNull == value) null
      else JsonColumnRep.IdClearNumberRep(value.asInstanceOf[SoQLID])
  }

  object VersionRep extends CsvColumnRep {
    def toString(value: SoQLValue) =
      if(SoQLNull == value) null
      else JsonColumnRep.VersionStringRep(value.asInstanceOf[SoQLVersion])
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

  class GeometryLikeRep[T <: Geometry](repType: SoQLType, geometry: SoQLValue => T) extends CsvColumnRep {
    def toString(value: SoQLValue) =
      if (SoQLNull == value) null
      else repType.asInstanceOf[SoQLGeometryLike[T]].WktRep(geometry(value))
  }

  object BlobRep extends CsvColumnRep {
    def toString(value: SoQLValue) =
      if(SoQLNull == value) null
      else value.asInstanceOf[SoQLBlob].value
  }

  object LocationRep extends CsvColumnRep {

    /**
     * The format is designed to look like OBE location format so that
     * csv file can be sent directly to caller w/o going through core.
     * address,
     * city, state zip
     */
    def toString(value: SoQLValue) = {
      value match {
        case SoQLNull => null
        case SoQLLocation(lat, lng, address) =>
          val sb = new StringBuilder
          address.foreach { (a: String) =>
            JsonUtil.parseJson[JObject](a) match {
              case Right(o@JObject(_)) => addressToCsv(sb, o)
              case _ =>
            }
          }
          (lat, lng) match {
            case (Some(y), Some(x)) =>
              if (sb.nonEmpty) { sb.append("\n") }
              sb.append(s"($y, $x)")
            case _ => null
          }
          if (sb.nonEmpty) sb.toString() else null
        case _ => null
      }
    }

    def addressToCsv(sb: StringBuilder, o: JObject): Unit = {
      Seq(("address", "\n"), ("city", ", "), ("state", " "), ("zip", "")).map { case (field, sep) =>
        o.get(field) match {
          case Some(JString(x)) => sb.append(x); sb.append(sep)
          case _ =>
        }
      }
    }
  }

  object PhoneRep extends CsvColumnRep {
    /**
     * phoneType: phoneNumber
     * phoneNumber (if phoneType is null)
     * phoneType (if phoneNumber is null)
     */
    def toString(value: SoQLValue) = {
      value match {
        case SoQLNull => null
        case SoQLPhone(phoneNumber, phoneType) =>
          Seq(phoneType, phoneNumber).flatten.mkString(": ")
        case _ => null
      }
    }
  }

  /**
    * Core is not supposed to use csv serialization format of document type directly.
    * It will ask for cjson and perform its own serialization.
    * Having an implementation will help development in some occasion.
    */
  object DocumentRep extends CsvColumnRep {
    /**
      * phoneType: phoneNumber
      * phoneNumber (if phoneType is null)
      * phoneType (if phoneNumber is null)
      */
    def toString(value: SoQLValue) = {
      value match {
        case SoQLNull => null
        case SoQLDocument(fileId, optContentType, optFilename) =>
          val sb = new StringBuilder
          sb.append(fileId)
          val hasFilename = optFilename.nonEmpty
          if (optContentType.nonEmpty || hasFilename) sb.append("?")
          optFilename.foreach { x =>
            sb.append("filename=")
            sb.append(URLEncoder.encode(x, "UTF-8"))
          }
          optContentType.foreach { x =>
            if (hasFilename) { sb.append("&") }
            sb.append("content_type=")
            sb.append(URLEncoder.encode(x, "UTF-8"))
          }
          sb.toString()
        case _ => null
      }
    }
  }

  object JsonRep extends CsvColumnRep {
    def toString(value: SoQLValue) =
      value match {
        case SoQLNull => null
        case SoQLJson(jv) => JsonUtil.renderJson(jv, pretty=false)
        case _ => null
      }
  }

  /**
    * Core is not supposed to use csv serialization format of photo type directly.
    * It will ask for cjson and perform its own serialization.
    * Having an implementation will help development in some occasion.
    */
  object PhotoRep extends CsvColumnRep {
    def toString(value: SoQLValue) =
      if(SoQLNull == value) null
      else value.asInstanceOf[SoQLPhoto].value
  }

  object UrlRep extends CsvColumnRep {
    def toString(value: SoQLValue) = {
      value match {
        case SoQLNull => null
        case SoQLUrl(Some(url), Some(description)) =>
          s"$description ($url)"
        case SoQLUrl(Some(url), None) =>
          url
        case SoQLUrl(None, Some(description)) =>
          description
        case _ => null
      }
    }
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
    SoQLObject -> ObjectRep,
    SoQLArray -> ArrayRep,
    SoQLJson -> JValueRep,
    SoQLPoint -> new GeometryLikeRep[Point](SoQLPoint, _.asInstanceOf[SoQLPoint].value),
    SoQLMultiLine -> new GeometryLikeRep[MultiLineString](SoQLMultiLine, _.asInstanceOf[SoQLMultiLine].value),
    SoQLMultiPolygon -> new GeometryLikeRep[MultiPolygon](SoQLMultiPolygon, _.asInstanceOf[SoQLMultiPolygon].value),
    SoQLLine -> new GeometryLikeRep[LineString](SoQLLine, _.asInstanceOf[SoQLLine].value),
    SoQLMultiPoint -> new GeometryLikeRep[MultiPoint](SoQLMultiPoint, _.asInstanceOf[SoQLMultiPoint].value),
    SoQLPolygon -> new GeometryLikeRep[Polygon](SoQLPolygon, _.asInstanceOf[SoQLPolygon].value),
    SoQLBlob -> BlobRep,
    SoQLPhone -> PhoneRep,
    SoQLLocation -> LocationRep,
    SoQLDocument -> DocumentRep,
    SoQLPhoto -> PhotoRep,
    SoQLUrl -> UrlRep,
    SoQLJson -> JsonRep
  )

  val forTypeClearId: Map[SoQLType, CsvColumnRep] =
    (forType - SoQLID) + (SoQLID -> ClearIDRep)
}
