package com.socrata.soda.server.export

import com.rojoma.json.v3.ast.{JValue, JNull, JObject, JString}
import com.rojoma.json.v3.io.{CompactJsonWriter, JsonReader}
import com.rojoma.simplearm.util._
import com.socrata.http.common.util.AliasedCharset
import com.socrata.soda.server.highlevel.ExportDAO
import com.socrata.soda.server.highlevel.ExportDAO.ColumnInfo
import com.socrata.soda.server.persistence.ColumnRecordLike
import com.socrata.soda.server.wiremodels.JsonColumnRep
import com.socrata.soql.types._
import com.socrata.thirdparty.geojson.JtsCodecs.geoCodec
import java.io.BufferedWriter
import javax.activation.MimeType
import javax.servlet.http.HttpServletResponse

/**
 * Exports rows as GeoJSON
 */
object GeoJsonExporter extends Exporter {
  val mimeTypeBase = "application/vnd.geo+json"
  val mimeType = new MimeType(mimeTypeBase)
  val extension = Some("geojson")

  // For now, GeoJSON only works if you have exactly ONE geo column in the dataset.
  // Attempting to export a dataset with zero or more than one geo columns will return HTTP 406.
  // For non-geo datasets the reason for this behavior is obvious.
  // For datasets with >1 geo column, we don't currently have a way for the user to specify which
  // column is the primary geo column that should be returned as the the top level "geometry" element
  // (others would be relegated to "properties"). Until we have prioritize implementing a way to
  // represent that in the store or pass it in the export request, we will error out.
  override def validForSchema(schema: Seq[ColumnRecordLike]): Boolean = {
    schema.count(_.typ.isInstanceOf[SoQLGeometryLike[_]]) == 1
  }

  def export(resp: HttpServletResponse,
             charset: AliasedCharset,
             schema: ExportDAO.CSchema,
             rows: Iterator[Array[SoQLValue]],
             singleRow: Boolean = false,
             obfuscateId: Boolean = true)
  {
    val mt = new MimeType(mimeTypeBase)
    mt.setParameter("charset", charset.alias)
    resp.setContentType(mt.toString)

    for {
      rawWriter <- managed(resp.getWriter)
      w <- managed(new BufferedWriter(rawWriter, 65536))
    } yield {
      val processor = new GeoJsonProcessor(w, schema, singleRow, obfuscateId)
      processor.go(rows)
    }
  }
}

/**
 * Generates GeoJSON from a schema
 */
class GeoJsonProcessor(writer: BufferedWriter, schema: ExportDAO.CSchema, singleRow: Boolean, obfuscateId: Boolean) {
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

  private def getGeoColumnIndex(columns: Seq[ColumnInfo]): Int = {
    val geoColumnIndices = columns.zipWithIndex.collect {
      case (columnInfo, index) if columnInfo.typ.isInstanceOf[SoQLGeometryLike[_]] => index
    }

    // We validate the dataset schema upfront and throw a 406 on datasets that don't
    // contain exactly one geo column, so theoretically this should never happen :/
    if (geoColumnIndices.size != 1) throw InvalidGeoJsonSchema

    geoColumnIndices(0)
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
    val properties = row.zipWithIndex.filterNot(_._2 == geoColumnIndex).map { case (value, index) =>
      propertyNames(index) -> propertyReps(index).toJValue(value)
    }

    val map = Map("type"       -> JString("Feature"),
                  "geometry"   -> getGeometryJson(row(geoColumnIndex)),
                  "properties" -> JObject(properties.toMap))
    val finalMap = if (singleRow) map + ("crs" -> JsonReader.fromString(wgs84ProjectionInfo)) else map

    jsonWriter.write(JObject(finalMap))
  }

  def go(rows: Iterator[Array[SoQLValue]]) {
    if (!singleRow) writer.write(featureCollectionPrefix)

    if(rows.hasNext) {
      writeGeoJsonRow(rows.next())
      if(singleRow && rows.hasNext) throw new IllegalArgumentException("Expect to get exactly one row but got more.")
    }

    while (rows.hasNext) {
      writer.write(",")
      writeGeoJsonRow(rows.next())
    }

    if(!singleRow) writer.write(featureCollectionSuffix)
  }
}

/**
 * Helpful objects related to GeoJSON processing
 */
object GeoJsonProcessor {
  object InvalidGeoJsonSchema extends Exception
}
