package com.socrata.soda.server.export

import com.rojoma.simplearm.v2.ResourceScope
import com.socrata.http.common.util.{HttpUtils, AliasedCharset}
import com.socrata.http.server.HttpResponse
import com.socrata.soda.server.highlevel.ExportDAO
import com.socrata.soda.server.persistence.ColumnRecordLike
import com.socrata.soda.server.wiremodels.JsonColumnRep
import com.socrata.http.server.responses._
import com.socrata.soql.types.{SoQLType, SoQLValue}
import java.util.Locale
import javax.activation.MimeType

trait Exporter {
  val mimeType: MimeType
  val extension: Option[String]
  def export(charset: AliasedCharset, schema: ExportDAO.CSchema,
             rows: Iterator[Array[SoQLValue]], singleRow: Boolean = false,
             obfuscateId: Boolean = true, bom: Boolean = false): HttpResponse
  protected def exporterHeaders(schema: ExportDAO.CSchema): HttpResponse =
    schema.lastModified.fold(NoOp) { lm =>
      Header("Last-Modified", HttpUtils.HttpDateFormat.print(lm))
    }
}

object Exporter {
  val exporters = List(JsonExporter, CJsonExporter, CsvExporter, GeoJsonExporter, SoQLPackExporter)
  val exportForMimeType = exporters.map { e => e.mimeType -> e }.toMap
  val exporterExtensions = exporters.flatMap(_.extension).map(canonicalizeExtension).toSet

  def canonicalizeExtension(s: String) = s.toLowerCase(Locale.US)
}
