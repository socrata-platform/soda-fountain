package com.socrata.soda.server.export

import com.socrata.http.common.util.{AliasedCharset, HttpUtils}
import com.socrata.http.server.HttpResponse
import com.socrata.soda.server.highlevel.ExportDAO
import com.socrata.http.server.responses._
import com.socrata.soql.types.SoQLValue
import java.util.Locale

import javax.activation.MimeType
import javax.servlet.http.HttpServletResponse
import com.rojoma.json.v3.ast.{JArray, JString}
import com.rojoma.json.v3.io.CompactJsonWriter
import com.socrata.http.server.implicits._
import com.socrata.soda.message.MessageProducer
import com.socrata.soda.server.id.ResourceName

trait Exporter {
  val mimeType: MimeType
  val extension: Option[String]
  def export(charset: AliasedCharset, schema: ExportDAO.CSchema,
             rows: Iterator[Array[SoQLValue]], singleRow: Boolean = false,
             obfuscateId: Boolean = true, bom: Boolean = false,
             fuseMap: Map[String, String] = Map.empty)(messageProducer: MessageProducer, entityIds: Seq[String]): HttpResponse
  protected def exporterHeaders(schema: ExportDAO.CSchema): HttpResponse =
    schema.lastModified.fold(NoOp) { lm =>
      Header("Last-Modified", HttpUtils.HttpDateFormat.print(lm)) ~> maybeSoda2FieldsHeader(schema)
    }

  /**
    * Override in specific exporters if we want X-SODA2-Fields
    */
  protected def maybeSoda2FieldsHeader(schema: ExportDAO.CSchema): HttpServletResponse => Unit = _ => { }

  protected def writeSoda2FieldsHeader(schema: ExportDAO.CSchema): HttpServletResponse => Unit = {
    val xhFields = "X-SODA2-Fields"
    val xhTypes = "X-SODA2-Types"
    val xhDeprecation = "X-SODA2-Warning"
    val xhLimit = 5000 // We have a 6k header size limit

    val soda2Fields = CompactJsonWriter.toString(JArray(schema.schema.map(ci => JString(ci.fieldName.name))))
    val soda2Types = CompactJsonWriter.toString(JArray(schema.schema.map(ci => JString(ci.typ.name.name))))

    val deprecatedSodaHeaders =
      if(soda2Fields.length + soda2Types.length < xhLimit) {
        Header(xhFields, soda2Fields) ~>
          Header(xhTypes, soda2Types) ~>
          Header(xhDeprecation, "X-SODA2-Fields and X-SODA2-Types are deprecated.  Use the c-json output format if you require this information.")
      } else {
        Header(xhDeprecation, "X-SODA2-Fields and X-SODA2-Types are deprecated and have been suppressed for being too large.  Use the c-json output format if you require this information.")
      }
    deprecatedSodaHeaders
  }
}

object Exporter {
  val exporters = List(JsonExporter, CJsonExporter, CsvExporter, TsvExporter, GeoJsonExporter, SoQLPackExporter)
  val exportForMimeType = exporters.map { e => e.mimeType -> e }.toMap
  val exporterExtensions = exporters.flatMap(_.extension).map(canonicalizeExtension).toSet

  def canonicalizeExtension(s: String) = s.toLowerCase(Locale.US)
}
