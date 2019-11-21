package com.socrata.soda.server.export

import com.rojoma.simplearm.v2._
import com.socrata.http.common.util.AliasedCharset
import com.socrata.http.server.HttpResponse
import com.socrata.soda.server.highlevel.ExportDAO
import com.socrata.soda.server.highlevel.ExportDAO.ColumnInfo
import com.socrata.soda.server.util.AdditionalJsonCodecs._
import com.socrata.soda.server.wiremodels.{JsonColumnRep, JsonColumnWriteRep}
import com.socrata.soql.SoQLPackWriter
import com.socrata.soql.types._
import java.io.DataOutputStream

import javax.activation.MimeType
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import com.socrata.soda.message.{MessageProducer, RowsLoadedApiMetricMessage}
import com.socrata.soda.server.id.ResourceName

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

  def export(charset: AliasedCharset,
             schema: ExportDAO.CSchema,
             rows: Iterator[Array[SoQLValue]],
             singleRow: Boolean = false,
             obfuscateId: Boolean = true,
             bom: Boolean = false,
             fuseMap: Map[String, String] = Map.empty)
            (messageProducer: MessageProducer, entityIds: Seq[String], accessType: Option[String]): HttpResponse = { // This format ignores obfuscateId.  SoQLPack does not obfuscate id.
    // Compute the schema
    val soqlSchema = schema.schema.map { ci =>
      (ci.fieldName.name, ci.typ)
    }.toSeq

    val rowCountElem: Option[(String, Long)] = schema.rowCount.map { count => "row_count" -> count }
    exporterHeaders(schema) ~> ContentType(mimeType) ~> Stream { os =>
      // TODO: update SoQLPackWriter so that we can write rows_loaded_api metric
      //       messageProducer.send(RowsLoadedApiMetricMessage(resourceName.name, ttl), raw = true)
      val writer = new SoQLPackWriter(soqlSchema, Seq(rowCountElem).flatten.toMap)
      val rowsCount = writer.write(os, rows)
      entityIds.foreach(id => messageProducer.send(RowsLoadedApiMetricMessage(id, rowsCount, accessType)))
    }
  }
}
