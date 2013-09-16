package com.socrata.soda.server.resources

import java.util.Locale

import com.socrata.http.server.routing.OptionallyTypedPathComponent
import com.socrata.soda.server.id.ResourceName
import com.socrata.soda.server.highlevel.ExportDAO
import com.socrata.soql.types.SoQLValue
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import com.socrata.soda.server.SodaUtils
import com.rojoma.simplearm.util._
import com.rojoma.json.ast.JString
import com.rojoma.json.io.CompactJsonWriter
import com.socrata.soda.server.wiremodels.{CsvColumnRep, CsvColumnWriteRep, JsonColumnRep, JsonColumnWriteRep}
import java.io.BufferedWriter

case class Export(exportDAO: ExportDAO) {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[Export])

  type Exporter = (HttpServletResponse, Seq[ExportDAO.ColumnInfo], Iterator[Array[SoQLValue]]) => Unit

  def jsonExporter(resp: HttpServletResponse, schema: Seq[ExportDAO.ColumnInfo], rows: Iterator[Array[SoQLValue]]) {
    log.info("TODO: Negotiate charset")
    resp.setContentType(SodaUtils.jsonContentTypeUtf8)
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
          if(row.length != 0) {
            writer.write(names(0))
            writer.write(':')
            jsonWriter.write(reps(0).toJValue(row(0)))
            var i = 1
            while(i != row.length) {
              writer.write(',')
              writer.write(names(i))
              writer.write(':')
              jsonWriter.write(reps(i).toJValue(row(i)))
              i += 1
            }
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

  def csvExporter(resp: HttpServletResponse, schema: Seq[ExportDAO.ColumnInfo], rows: Iterator[Array[SoQLValue]]) {
    log.info("TODO: Negotiate charset")
    resp.setContentType("text/csv; charset=utf-8")
    using(resp.getWriter) { w =>
      class Processor {
        val writer = w
        val reps: Array[CsvColumnWriteRep] = schema.map { f => CsvColumnRep.forType(f.typ) }.toArray
        val sb = new java.lang.StringBuilder

        def writeCSVRow(row: Array[String]) {
          sb.setLength(0)
          var i = 0
          while(i != row.length) {
            val cell = row(i)
            if(cell != null) {
              sb.append('"')
              var j = 0
              while(j != cell.length) {
                val c = cell.charAt(j)
                if(c == '"') sb.append(c)
                sb.append(c)
                j += 1
              }
              sb.append('"')
            }

            sb.append(',')
            i += 1
          }
          if(i != 0) sb.setLength(sb.length - 1) // remove last comma
          sb.append('\n')
          writer.write(sb.toString)
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

  val exporters = Map[String, Exporter](
    "json" -> jsonExporter _,
    "csv" -> csvExporter _
  )
  val defaultFormat = "json"

  def export(resourceName: ResourceName, ext: Option[String])(req: HttpServletRequest)(resp: HttpServletResponse) {
    log.info("TODO: Smarter content-type negotiation")
    val exporter = exporters(ext.map(canonicalizeExtension).getOrElse(defaultFormat))
    exportDAO.export(resourceName) {
      case ExportDAO.Success(schema, rows) =>
        resp.setStatus(HttpServletResponse.SC_OK)
        exporter(resp, schema, rows)
    }
  }

  case class service(resourceAndExt: OptionallyTypedPathComponent[ResourceName]) extends SodaResource {
    override def get = export(resourceAndExt.value, resourceAndExt.extension.map(canonicalizeExtension))
  }

  def canonicalizeExtension(s: String) = s.toLowerCase(Locale.US)
  def extensions(s: String) = exporters.contains(canonicalizeExtension(s))
}
