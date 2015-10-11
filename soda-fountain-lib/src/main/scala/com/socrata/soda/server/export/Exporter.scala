package com.socrata.soda.server.export

import com.socrata.http.common.util.AliasedCharset
import com.socrata.soda.server.highlevel.ExportDAO
import com.socrata.soda.server.persistence.ColumnRecordLike
import com.socrata.soql.types.SoQLValue
import java.util.Locale
import javax.activation.MimeType
import javax.servlet.http.HttpServletResponse

trait Exporter {

  type ArrayLike[T] = {
    def apply(i: Int): T

    def length: Int
  }

  val mimeType: MimeType
  val extension: Option[String]
  def export(resp: HttpServletResponse, charset: AliasedCharset, schema: ExportDAO.CSchema, rows: Iterator[ArrayLike[SoQLValue]], singleRow: Boolean = false)
  def validForSchema(schema: Seq[ColumnRecordLike]): Boolean = true
}

object Exporter {
  val exporters = List(JsonExporter, CJsonExporter, CsvExporter, GeoJsonExporter, SoQLPackExporter)
  val exportForMimeType = exporters.map { e => e.mimeType -> e }.toMap
  val exporterExtensions = exporters.flatMap(_.extension).map(canonicalizeExtension).toSet

  def canonicalizeExtension(s: String) = s.toLowerCase(Locale.US)
}
