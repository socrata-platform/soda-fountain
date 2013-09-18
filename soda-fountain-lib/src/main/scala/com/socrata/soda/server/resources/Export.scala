package com.socrata.soda.server.resources

import java.util.Locale

import com.socrata.http.server.routing.OptionallyTypedPathComponent
import com.socrata.soda.server.id.ResourceName
import com.socrata.soda.server.highlevel.ExportDAO
import com.socrata.soql.types.SoQLValue
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import com.socrata.soda.server.SodaUtils
import com.rojoma.simplearm.util._
import com.rojoma.json.ast.{JNull, JString}
import com.rojoma.json.io.CompactJsonWriter
import com.socrata.soda.server.wiremodels.{CsvColumnRep, CsvColumnWriteRep, JsonColumnRep, JsonColumnWriteRep}
import java.io.BufferedWriter
import javax.activation.MimeType
import com.socrata.http.common.util.{AliasedCharset, ContentNegotiation}
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._

case class Export(exportDAO: ExportDAO) {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[Export])

  trait Exporter {
    val mimeType: MimeType
    val extension: Option[String]
    def export(resp: HttpServletResponse, charset: AliasedCharset, schema: Seq[ExportDAO.ColumnInfo], rows: Iterator[Array[SoQLValue]])
  }

  object JsonExporter extends Exporter {
    val mimeTypeBase = SodaUtils.jsonContentTypeBase
    val mimeType = new MimeType(mimeTypeBase)
    val extension = Some("json")

    def export(resp: HttpServletResponse, charset: AliasedCharset, schema: Seq[ExportDAO.ColumnInfo], rows: Iterator[Array[SoQLValue]]) {
      log.info("TODO: Negotiate charset")
      val mt = new MimeType(mimeTypeBase)
      mt.setParameter("charset", charset.alias)
      resp.setContentType(mt.toString)
      for {
        rawWriter <- managed(resp.getWriter)
        w <- managed(new BufferedWriter(rawWriter, 65536))
      } {
        class Processor {
          val writer = w
          val jsonWriter = new CompactJsonWriter(writer)
          val names: Array[String] = schema.map { ci => JString(ci.fieldName.name).toString }.toArray
          val reps: Array[JsonColumnWriteRep] = schema.map { ci => JsonColumnRep.forClientType(ci.typ) }.toArray

          def writeJsonRow(row: Array[SoQLValue]) {
            writer.write('{')
            var didOne = false
            var i = 0
            while(i != row.length) {
              val jsonized = reps(i).toJValue(row(i))
              if(JNull != jsonized) {
                if(didOne) writer.write(',')
                else didOne = true
                writer.write(names(i))
                writer.write(':')
                jsonWriter.write(reps(i).toJValue(row(i)))
              }
              i += 1
            }
            writer.write('}')
          }

          def go(rows: Iterator[Array[SoQLValue]]) {
            writer.write('[')
            if(rows.hasNext) writeJsonRow(rows.next())
            while(rows.hasNext) {
              writer.write("\n,")
              writeJsonRow(rows.next())
            }
            writer.write("\n]\n")
          }
        }
        val processor = new Processor
        processor.go(rows)
      }
    }
  }

  object CsvExporter extends Exporter {
    val mimeTypeBase = "text/csv"
    val mimeType = new MimeType(mimeTypeBase)
    val extension = Some("csv")

    def export(resp: HttpServletResponse, charset: AliasedCharset, schema: Seq[ExportDAO.ColumnInfo], rows: Iterator[Array[SoQLValue]]) {
      log.info("TODO: Negotiate charset")
      val mt = new MimeType(mimeTypeBase)
      mt.setParameter("charset", charset.alias)
      resp.setContentType(mt.toString)
      for {
        rawWriter <- managed(resp.getWriter)
        w <- managed(new BufferedWriter(rawWriter, 65536))
      } yield {
        class Processor {
          val writer = w
          val reps: Array[CsvColumnWriteRep] = schema.map { f => CsvColumnRep.forType(f.typ) }.toArray
          val sb = new java.lang.StringBuilder

          // somewhat surprisingly, writing cells into a stringbuilder and then
          // dumping the result to the writer is slightly faster than writing
          // straight to the writer, even though it's a BufferedWriter.
          def writeCell(cell: String) {
            if(cell != null) {
              sb.setLength(0)
              sb.append('"')
              var j = 0
              while(j != cell.length) {
                val c = cell.charAt(j)
                if(c == '"') sb.append(c)
                sb.append(c)
                j += 1
              }
              sb.append('"')
              writer.write(sb.toString)
            }
          }

          def writeCSVRow(row: Array[String]) {
            if(row.length != 0) {
              writeCell(row(0))
              var i = 1
              while(i != row.length) {
                writer.write(',')
                writeCell(row(i))
                i += 1
              }
            }
            writer.write('\n')
          }

          def convertInto(out: Array[String], row: Array[SoQLValue]) {
            var i = 0
            while(i != out.length) {
              out(i) = reps(i).toString(row(i))
              i += 1
            }
          }

          def go(rows: Iterator[Array[SoQLValue]]) {
            val array = schema.map(_.humanName).toArray
            writeCSVRow(array)
            while(rows.hasNext) {
              convertInto(array, rows.next())
              writeCSVRow(array)
            }
          }
        }
        val processor = new Processor
        processor.go(rows)
      }
    }
  }

  val exporters = List(JsonExporter, CsvExporter)

  val exportForMimeType = exporters.map { e => e.mimeType -> e }.toMap
  val exporterExtensions = exporters.flatMap(_.extension).map(canonicalizeExtension).toSet

  implicit val contentNegotiation = new ContentNegotiation(exporters.map { exp => exp.mimeType -> exp.extension }, List("en-US"))

  def export(resourceName: ResourceName, ext: Option[String])(req: HttpServletRequest)(resp: HttpServletResponse) {
    req.negotiateContent match {
      case Some((mimeType, charset, language)) =>
        val exporter = exportForMimeType(mimeType)
        exportDAO.export(resourceName) {
          case ExportDAO.Success(schema, rows) =>
            resp.setStatus(HttpServletResponse.SC_OK)
            resp.setHeader("Vary", ContentNegotiation.headers.mkString(","))
            exporter.export(resp, charset, schema, rows)
        }
      case None =>
        // TODO better error
        NotAcceptable(resp)
    }
  }

  case class service(resourceAndExt: OptionallyTypedPathComponent[ResourceName]) extends SodaResource {
    override def get = export(resourceAndExt.value, resourceAndExt.extension.map(canonicalizeExtension))
  }

  def canonicalizeExtension(s: String) = s.toLowerCase(Locale.US)
  def extensions(s: String) = exporterExtensions.contains(canonicalizeExtension(s))
}
