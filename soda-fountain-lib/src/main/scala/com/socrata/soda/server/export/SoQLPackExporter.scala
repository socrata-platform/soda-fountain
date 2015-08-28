package com.socrata.soda.server.export

import com.rojoma.simplearm.util._
import com.socrata.http.common.util.AliasedCharset
import com.socrata.soda.server.highlevel.ExportDAO
import com.socrata.soda.server.highlevel.ExportDAO.ColumnInfo
import com.socrata.soda.server.util.AdditionalJsonCodecs._
import com.socrata.soda.server.wiremodels.{JsonColumnRep, JsonColumnWriteRep}
import com.socrata.soql.SoQLPackWriter
import com.socrata.soql.types._
import java.io.DataOutputStream
import javax.activation.MimeType
import javax.servlet.http.HttpServletResponse

/**
 * Exports in SoQLPack format - an efficient, MessagePack-based SoQL transport medium.
 * For more details, see soql-reference README and SoQLPackWriter etc.
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

  def export(resp: HttpServletResponse,
             charset: AliasedCharset,
             schema: ExportDAO.CSchema,
             rows: Iterator[Array[SoQLValue]],
             singleRow: Boolean = false,
             options: Map[String, String] = Map()) {
    val mt = new MimeType(mimeTypeBase)
    resp.setContentType(mt.toString)
    for {
      os <- managed(resp.getOutputStream)
    } yield {
      // Compute the schema
      val soqlSchema = schema.schema.map { ci =>
        (ci.fieldName.name, ci.typ)
      }.toSeq

      val rowCountElem: Option[(String, Long)] = schema.rowCount.map { count => "row_count" -> count }
      val writer = new SoQLPackWriter(soqlSchema, Seq(rowCountElem).flatten.toMap)
      writer.write(os, rows)
    }
  }
}