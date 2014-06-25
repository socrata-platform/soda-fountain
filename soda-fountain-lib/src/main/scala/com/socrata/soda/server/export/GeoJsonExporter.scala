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

object GeoJsonExporter extends Exporter {
  val mimeTypeBase = "application/vnd.geo+json"
  val mimeType = new MimeType(mimeTypeBase)
  val extension = Some("geojson")

  object InvalidGeoJsonSchema extends Exception
  object InvalidGeoJsonRow extends Exception

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
        val featureCollectionPrefix = """{ "type": "FeatureCollection",
                                         |    "features": [""".stripMargin
        val featureCollectionSuffix = """] }"""

        val (geomField, otherFields) = splitSchema
        val names = otherFields.map { ci => ci.fieldName.name }
        val reps = otherFields.map { ci => JsonColumnRep.forClientType(ci.typ) }

        val writer = w
        val jsonWriter = new CompactJsonWriter(writer)

        private def splitSchema: (ExportDAO.ColumnInfo, Seq[ExportDAO.ColumnInfo]) = {
          val (geomField, otherFields) = schema.schema.partition(_.typ.isInstanceOf[SoQLGeometryLike[_]])

          // If we are validating the dataset schema upfront and throwing a 406 on datasets
          // that don't contain exactly one geo column, this should never happen :/
          if (geomField.size != 1) throw InvalidGeoJsonSchema

          (geomField(0), otherFields)
        }

        private def splitGeometryAndProperties(row: Array[SoQLValue]): (SoQLValue, Array[SoQLValue]) = {
          row.foreach { field =>
            println(s"${field.typ},${field.typ.isInstanceOf[SoQLGeometryLike[_]]},${field.toString}")
          }
          val (geom, properties) = row.partition(_.typ.isInstanceOf[SoQLGeometryLike[_]])

          // If we are validating the dataset schema upfront and throwing a 406 on datasets
          // that don't contain exactly one geo column, this should never happen :/
          println(row.map(_.toString).mkString(","))
          if (geom.size != 1) throw InvalidGeoJsonRow

          (geom(0), properties)
        }

        def writeGeoJsonRow(row: Array[SoQLValue]) {
          val (soqlGeom, soqlProperties) = splitGeometryAndProperties(row)
          val geomJson = soqlGeom match {
            case SoQLPoint(p)         => SoQLPoint.JsonRep.apply(p)
            case SoQLMultiLine(ml)    => SoQLMultiLine.JsonRep.apply(ml)
            case SoQLMultiPolygon(mp) => SoQLMultiPolygon.JsonRep.apply(mp)
          }

          val rowData = (names, reps, soqlProperties).zipped
          val properties = rowData.map { (name, rep, soqlProperty) => name -> rep.toJValue(soqlProperty) }

          val map = JObject(Map("type" -> JString("Feature"),
                                "geometry" -> JsonReader.fromString(geomJson),
                                "properties" -> JObject(properties.toMap)))
          jsonWriter.write(map)
        }

        def go(rows: Iterator[Array[SoQLValue]]) {
          // TODO : Find a cleaner way to stream the geometry collection and not keep the whole thing in memory
          if (!singleRow) writer.write(featureCollectionPrefix)

          if(rows.hasNext) {
            writeGeoJsonRow(rows.next())
            if(singleRow && rows.hasNext) throw new Exception("Expect to get exactly one row but got more.")
          }

          while (rows.hasNext) {
            writer.write("\n,")
            writeGeoJsonRow(rows.next())
          }

          if(!singleRow) writer.write(s"\n$featureCollectionSuffix\n")
          else writer.write("\n")
        }
      }
      val processor = new Processor
      processor.go(rows)
    }
  }
}
