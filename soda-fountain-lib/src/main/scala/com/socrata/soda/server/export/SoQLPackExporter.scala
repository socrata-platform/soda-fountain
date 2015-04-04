package com.socrata.soda.server.export

import com.rojoma.simplearm.util._
import com.socrata.http.common.util.AliasedCharset
import com.socrata.soda.server.highlevel.ExportDAO
import com.socrata.soda.server.highlevel.ExportDAO.ColumnInfo
import com.socrata.soda.server.util.AdditionalJsonCodecs._
import com.socrata.soda.server.wiremodels.{JsonColumnRep, JsonColumnWriteRep}
import com.socrata.soql.types._
import com.vividsolutions.jts.io.WKBWriter
import java.io.DataOutputStream
import javax.activation.MimeType
import javax.servlet.http.HttpServletResponse
import org.velvia.MsgPack

/**
 * Exports in SoQLPack format - an efficient, MessagePack-based SoQL transport medium.
 *   - Much more efficient than CJSON, GeoJSON, etc... especially for geometries
 *   - Designed to be very streaming friendly
 *   - MessagePack format means easier to implement clients in any language
 *
 * The structure of the content is pretty much identical to CJSON,
 * but framed as follows:
 *   +0000  P bytes - CJSON-like header, MessagePack object/map, currently with the following entries:
 *                        "row_count" -> integer count of rows
 *                        "geometry_index" -> index within row of geometry shape
 *                        "schema" -> MsgPack equiv of [{"c": fieldName1, "t": fieldType1}, ...]
 *   +P     R bytes - first row - MessagePack array
 *                        geometries are WKB binary blobs
 *                        Text fields are strings
 *                        Number fields are numbers
 *                        All other fields... TBD
 *   +P+R           - second row
 *   All other rows are encoded as MessagePack arrays and follow sequentially.
 *
 *   NOTE: Unlike CJSON, the columns are not rearranged in alphabetical field order, but the original
 *   order in the Array[SoQLValue] and CSchema are retained.
 */
object SoQLPackExporter extends Exporter {
  // Technically we could use "application/x-msgpack", but this is not entirely pure messagepack
  // to aid in streaming.
  val mimeTypeBase = "application/octet-stream"
  val mimeType = new MimeType(mimeTypeBase)
  val extension = Some("soqlpack")

  private def getGeoColumnIndex(columns: Seq[ColumnInfo]): Int = {
    val geoColumnIndices = columns.zipWithIndex.collect {
      case (columnInfo, index) if columnInfo.typ.isInstanceOf[SoQLGeometryLike[_]] => index
    }

    // Just return -1 if no geo column or more than one
    if (geoColumnIndices.size != 1) return -1

    geoColumnIndices(0)
  }

  def export(resp: HttpServletResponse, charset: AliasedCharset, schema: ExportDAO.CSchema, rows: Iterator[Array[SoQLValue]], singleRow: Boolean = false) {
    val mt = new MimeType(mimeTypeBase)
    resp.setContentType(mt.toString)
    for {
      os <- managed(resp.getOutputStream)
      dos <- managed(new DataOutputStream(os))
    } yield {
      val geoColumn = getGeoColumnIndex(schema.schema)

      val rowCountElem: Option[(String, Long)] = schema.rowCount.map { count => "row_count" -> count }
      val geoIndexElem = Some("geometry_index" -> geoColumn)

      val schemaMaps = schema.schema.map { ci =>
        Map("c" -> ci.fieldName.name, "t" -> ci.typ.toString)
      }.toSeq

      val headerMap: Map[String, Any] = Seq[Option[(String, Any)]](
                                          rowCountElem,
                                          geoIndexElem,
                                          Some("schema" -> schemaMaps)).flatten.toMap
      MsgPack.pack(headerMap, dos)

      // end of header

      val wkbWriter = new WKBWriter

      val reps: Array[JsonColumnWriteRep] = schema.schema.map { ci => JsonColumnRep.forClientType(ci.typ) }.toArray
      for (row <- rows) {
        val values: Seq[Any] = (0 until row.length).map { i =>
          // TODO: turn this logic into a MessagePackColumnRep
          row(i) match {
            case SoQLPoint(pt)       => wkbWriter.write(pt)
            case SoQLMultiLine(ml)   => wkbWriter.write(ml)
            case SoQLMultiPolygon(p) => wkbWriter.write(p)
            case SoQLText(str)       => str
            case SoQLNull            => null
            case null                => null
            case SoQLBoolean(bool)   => bool
            // For anything else, we rely on the JsonColumnRep and translate from JValues
            case other: SoQLValue    => ???
          }
        }
        MsgPack.pack(values, dos)
      }
      dos.flush()
    }
  }
}