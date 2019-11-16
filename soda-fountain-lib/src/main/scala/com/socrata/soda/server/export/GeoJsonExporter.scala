package com.socrata.soda.server.export

import com.rojoma.json.v3.ast.{JNull, JObject, JString, JValue}
import com.rojoma.json.v3.io.{CompactJsonWriter, JsonReader}
import com.rojoma.simplearm.v2._
import com.socrata.http.common.util.AliasedCharset
import com.socrata.http.server.HttpResponse
import com.socrata.soda.server.SodaInternalException
import com.socrata.soda.server.highlevel.ExportDAO
import com.socrata.soda.server.highlevel.ExportDAO.ColumnInfo
import com.socrata.soda.server.persistence.ColumnRecordLike
import com.socrata.soda.server.wiremodels.JsonColumnRep
import com.socrata.soql.types._
import com.socrata.thirdparty.geojson.JtsCodecs.geoCodec
import java.io.BufferedWriter

import javax.activation.MimeType
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import com.socrata.soda.message.{MessageProducer, RowsLoadedApiMetricMessage}
import com.socrata.soda.server.id.ResourceName

/**
 * Exports rows as GeoJSON
 */
object GeoJsonExporter extends Exporter {
  val mimeTypeBase = "application/vnd.geo+json"
  val mimeType = new MimeType(mimeTypeBase)
  val extension = Some("geojson")

  def export(charset: AliasedCharset,
             schema: ExportDAO.CSchema,
             rows: Iterator[Array[SoQLValue]],
             singleRow: Boolean = false,
             obfuscateId: Boolean = true,
             bom: Boolean = false,
             fuseMap: Map[String, String] = Map.empty)
            (messageProducer: MessageProducer, resourceName: ResourceName): HttpResponse = {
    val mt = new MimeType(mimeTypeBase)
    mt.setParameter("charset", charset.alias)
    exporterHeaders(schema) ~> Write(mt) { rawWriter =>
      using(new BufferedWriter(rawWriter, 65536)) { w =>
        val processor = new GeoJsonProcessor(w, schema, singleRow, obfuscateId, messageProducer, resourceName)
        processor.go(rows)
      }
    }
  }
}

/**
 * Generates GeoJSON from a schema
 */
class GeoJsonProcessor(writer: BufferedWriter, schema: ExportDAO.CSchema, singleRow: Boolean, obfuscateId: Boolean, messageProducer: MessageProducer, resourceName: ResourceName) {
  import GeoJsonProcessor._

  val wgs84ProjectionInfo = """{ "type": "name", "properties": { "name": "urn:ogc:def:crs:OGC:1.3:CRS84" } }"""
  val featureCollectionPrefix = """{ "type": "FeatureCollection", "features": ["""
  val featureCollectionSuffix = s"""], "crs" : $wgs84ProjectionInfo }"""

  val jsonColumnReps = if (obfuscateId) JsonColumnRep.forClientType
                       else JsonColumnRep.forClientTypeClearId
  val geoColumnIndex = getGeoColumnIndex(schema.schema)
  val propertyNames = schema.schema.map { ci => ci.fieldName.name }
  val propertyReps = schema.schema.map { ci => jsonColumnReps(ci.typ) }

  val jsonWriter = new CompactJsonWriter(writer)

  private def getGeoColumnIndex(columns: Seq[ColumnInfo]): Option[Int] = {
    val geoColumnIndices = columns.zipWithIndex.collect {
      case (columnInfo, index) if columnInfo.typ.isInstanceOf[SoQLGeometryLike[_]] => index
    }

    if (geoColumnIndices.size == 0) {
      // There are no geometry columns on this dataset.
      None
    } else {
      // As a first step to supporting geojson export on datasets with multiple geo columns,
      // we'll pick the first geo column we happened to encounter as the primary feature geometry.
      // Other geometry column values will be relegated to the attributes section of the feature.
      // Ideally we'd allow the API caller to pass a parameter indicating which column should be
      // the primary geometry, but that's a bigger refactor that we haven't prioritized yet.
      Some(geoColumnIndices(0))
    }
  }

  private def getGeometryJson(soqlGeom: SoQLValue): JValue =

    soqlGeom match {
      case SoQLPoint(p)         => geoCodec.encode(p)
      case SoQLMultiPoint(mp)   => geoCodec.encode(mp)
      case SoQLLine(l)          => geoCodec.encode(l)
      case SoQLMultiLine(ml)    => geoCodec.encode(ml)
      case SoQLPolygon(p)       => geoCodec.encode(p)
      case SoQLMultiPolygon(mp) => geoCodec.encode(mp)
      case _                    => JNull
    }

  private def writeGeoJsonRow(row: Array[SoQLValue]) {
    val properties = row.zipWithIndex.filterNot(_._2 == geoColumnIndex.getOrElse(-1)).map { case (value, index) =>
      propertyNames(index) -> propertyReps(index).toJValue(value)
    }

    val primaryGeometry = geoColumnIndex match {
      case Some(idx) => getGeometryJson(row(idx))
      case None      => JNull
    }

    val map = Map("type"       -> JString("Feature"),
                  "geometry"   -> primaryGeometry,
                  "properties" -> JObject(properties.toMap))
    val finalMap = if (singleRow) map + ("crs" -> JsonReader.fromString(wgs84ProjectionInfo)) else map

    jsonWriter.write(JObject(finalMap))
  }

  def go(rows: Iterator[Array[SoQLValue]]) {
    if (!singleRow) writer.write(featureCollectionPrefix)
    var ttl = 0
    if(rows.hasNext) {
      ttl += 1
      writeGeoJsonRow(rows.next())
      if(singleRow && rows.hasNext) throw new IllegalArgumentException("Expect to get exactly one row but got more.")
    }

    while (rows.hasNext) {
      ttl += 1
      writer.write(",")
      writeGeoJsonRow(rows.next())
    }

    if(!singleRow) writer.write(featureCollectionSuffix)
    messageProducer.send(RowsLoadedApiMetricMessage(resourceName.name, ttl), raw = true)
  }
}

/**
 * Helpful objects related to GeoJSON processing
 */
object GeoJsonProcessor {
  object InvalidGeoJsonSchema extends SodaInternalException("Invalid geojson schema")
}
