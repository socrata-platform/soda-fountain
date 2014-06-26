package com.socrata.soda.server.export

import com.rojoma.json.ast.{JObject, JString}
import com.rojoma.json.io.{CompactJsonWriter, JsonReader}
import com.rojoma.simplearm.util._
import com.socrata.http.common.util.AliasedCharset
import com.socrata.soda.server.highlevel.ExportDAO
import com.socrata.soda.server.wiremodels.JsonColumnRep
import com.socrata.soql.types._
import java.io.BufferedWriter
import javax.activation.MimeType
import javax.servlet.http.HttpServletResponse
import com.socrata.soda.server.persistence.ColumnRecord

object GeoJsonExporter extends Exporter {
  val mimeTypeBase = "application/vnd.geo+json"
  val mimeType = new MimeType(mimeTypeBase)
  val extension = Some("geojson")

  object InvalidGeoJsonSchema extends Exception

  override def validForSchema(schema: Seq[ColumnRecord]): Boolean = {
    schema.filter(col => col.typ.isInstanceOf[SoQLGeometryLike[_]]).size == 1
  }

  def export(resp: HttpServletResponse,
             charset: AliasedCharset,
             schema: ExportDAO.CSchema,
             rows: Iterator[Array[SoQLValue]],
             singleRow: Boolean = false) {
    val mt = new MimeType(mimeTypeBase)
    mt.setParameter("charset", charset.alias)
    resp.setContentType(mt.toString)

    // TODO : Return HTTP 406 if the dataset doesn't contain exactly one geo column.
    // This should happen in Resource before retrieving rows,
    // so we aren't pulling a large dataset from secondary
    // only to immediately throw it away.

      for {
      rawWriter <- managed(resp.getWriter)
      w <- managed(new BufferedWriter(rawWriter, 65536))
    } yield {
      class Processor {
        val wgs84ProjectionInfo = """{ "type": "name", "properties": { "name": "urn:ogc:def:crs:OGC:1.3:CRS84" } }"""
        val featureCollectionPrefix = """{ "type": "FeatureCollection", "features": ["""
        val featureCollectionSuffix = s"""], "crs" : $wgs84ProjectionInfo }"""

        val (geometry, properties) = splitOutGeoColumn[ExportDAO.ColumnInfo](schema.schema, ci => ci.typ)
        val propertyNames = properties.map { ci => ci.fieldName.name }
        val propertyReps = properties.map { ci => JsonColumnRep.forClientType(ci.typ) }

        val writer = w
        val jsonWriter = new CompactJsonWriter(writer)

        private def splitOutGeoColumn[T](columns: Seq[T], getSoQLType: T => SoQLType): (T, Seq[T]) = {
          val (geom, other) = columns.partition(getSoQLType(_).isInstanceOf[SoQLGeometryLike[_]])
          // Once we are validating the dataset schema upfront and throwing a 406 on datasets
          // that don't contain exactly one geo column, this should never happen :/
          if (geom.size != 1) throw InvalidGeoJsonSchema
          (geom(0), other)
        }

        private def getGeometryJson(soqlGeom: SoQLValue) = {
          val geomJson = soqlGeom match {
            case SoQLPoint(p)         => SoQLPoint.JsonRep.apply(p)
            case SoQLMultiLine(ml)    => SoQLMultiLine.JsonRep.apply(ml)
            case SoQLMultiPolygon(mp) => SoQLMultiPolygon.JsonRep.apply(mp)
          }
          JsonReader.fromString(geomJson)
        }

        private def writeGeoJsonRow(row: Array[SoQLValue]) {
          val (soqlGeom, soqlProperties) = splitOutGeoColumn[SoQLValue](row, field => field.typ)
          val rowData = (propertyNames, propertyReps, soqlProperties).zipped
          val properties = rowData.map { (name, rep, soqlProperty) => name -> rep.toJValue(soqlProperty) }
          val map = Map("type" -> JString("Feature"),
                        "geometry" -> getGeometryJson(soqlGeom),
                        "properties" -> JObject(properties.toMap))
          val finalMap = if (singleRow) map + ("crs" -> JsonReader.fromString(wgs84ProjectionInfo)) else map
          jsonWriter.write(JObject(finalMap))
        }

        def go(rows: Iterator[Array[SoQLValue]]) {
          if (!singleRow) writer.write(featureCollectionPrefix)

          if(rows.hasNext) {
            writeGeoJsonRow(rows.next())
            if(singleRow && rows.hasNext) throw new Exception("Expect to get exactly one row but got more.")
          }

          while (rows.hasNext) {
            writer.write(",")
            writeGeoJsonRow(rows.next())
          }

          if(!singleRow) writer.write(featureCollectionSuffix)
        }
      }
      val processor = new Processor
      processor.go(rows)
    }
  }
}
