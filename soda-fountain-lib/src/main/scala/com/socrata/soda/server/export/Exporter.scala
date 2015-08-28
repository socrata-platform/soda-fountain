package com.socrata.soda.server.export

import com.socrata.http.common.util.AliasedCharset
import com.socrata.soda.server.highlevel.ExportDAO
import com.socrata.soda.server.persistence.ColumnRecordLike
import com.socrata.soql.types.SoQLValue
import java.util.Locale
import javax.activation.MimeType
import javax.servlet.http.HttpServletResponse
import com.socrata.http.server.HttpRequest

trait Exporter {
  val mimeType: MimeType
  val extension: Option[String]
  def export(resp: HttpServletResponse,
    charset: AliasedCharset,
    schema: ExportDAO.CSchema,
    rows: Iterator[Array[SoQLValue]],
    singleRow: Boolean = false,
    options: Map[String, String] = Map()
  )
  def validForSchema(schema: Seq[ColumnRecordLike]): Boolean = true
  def pluckOptions(req: HttpRequest): Map[String, String] = Map()
}

object Exporter {
  val exporters = List(JsonExporter, CJsonExporter, CsvExporter, GeoJsonExporter, SoQLPackExporter)
  val exportForMimeType = exporters.map { e => e.mimeType -> e }.toMap
  val exporterExtensions = exporters.flatMap(_.extension).map(canonicalizeExtension).toSet

  def canonicalizeExtension(s: String) = s.toLowerCase(Locale.US)
}
