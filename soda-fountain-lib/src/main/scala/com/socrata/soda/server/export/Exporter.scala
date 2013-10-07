package com.socrata.soda.server.export

import com.socrata.soda.server.highlevel.ExportDAO
import com.socrata.soql.types.SoQLValue
import com.socrata.soda.server.SodaUtils
import com.socrata.http.common.util.AliasedCharset
import com.socrata.soda.server.util.AdditionalJsonCodecs._
import com.socrata.soda.server.wiremodels.{CsvColumnRep, CsvColumnWriteRep, JsonColumnRep, JsonColumnWriteRep}
import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.io.CompactJsonWriter
import com.rojoma.json.ast.{JNumber, JNull, JString}
import com.rojoma.simplearm.util._
import java.io.BufferedWriter
import java.util.Locale
import javax.activation.MimeType
import javax.servlet.http.HttpServletResponse

trait Exporter {
  val mimeType: MimeType
  val extension: Option[String]
  def export(resp: HttpServletResponse, charset: AliasedCharset, schema: ExportDAO.CSchema, rows: Iterator[Array[SoQLValue]], singleRow: Boolean = false)
}

object Exporter {
  val exporters = List(JsonExporter, CJsonExporter, CsvExporter)
  val exportForMimeType = exporters.map { e => e.mimeType -> e }.toMap
  val exporterExtensions = exporters.flatMap(_.extension).map(canonicalizeExtension).toSet

  def canonicalizeExtension(s: String) = s.toLowerCase(Locale.US)
}

object JsonExporter extends Exporter {
  val mimeTypeBase = SodaUtils.jsonContentTypeBase
  val mimeType = new MimeType(mimeTypeBase)
  val extension = Some("json")

  def export(resp: HttpServletResponse, charset: AliasedCharset, schema: ExportDAO.CSchema, rows: Iterator[Array[SoQLValue]], singleRow: Boolean = false) {
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
        val names: Array[String] = schema.schema.map { ci => JString(ci.fieldName.name).toString }.toArray
        val reps: Array[JsonColumnWriteRep] = schema.schema.map { ci => JsonColumnRep.forClientType(ci.typ) }.toArray

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
          if(!singleRow) writer.write('[')
          if(rows.hasNext) {
            writeJsonRow(rows.next())
            if(singleRow && rows.hasNext) throw new Exception("Expect to get exactly one row but got more.")
          }
          while(rows.hasNext) {
            writer.write("\n,")
            writeJsonRow(rows.next())
          }
          if(!singleRow) writer.write("\n]\n")
          else writer.write("\n")
        }
      }
      val processor = new Processor
      processor.go(rows)
    }
  }
}

object CJsonExporter extends Exporter {
  val mimeTypeBase = "application/json+x-socrata-cjson"
  val mimeType = new MimeType(mimeTypeBase)
  val extension = Some("cjson")

  def export(resp: HttpServletResponse, charset: AliasedCharset, schema: ExportDAO.CSchema, rows: Iterator[Array[SoQLValue]], singleRow: Boolean = false) {
    val mt = new MimeType(mimeTypeBase)
    mt.setParameter("charset", charset.alias)
    resp.setContentType(mt.toString)
    for {
      rawWriter <- managed(resp.getWriter)
      w <- managed(new BufferedWriter(rawWriter, 65536))
    } yield {
      val jw = new CompactJsonWriter(w)
      val schemaOrdering = schema.schema.zipWithIndex.sortBy(_._1.fieldName).map(_._2).toArray
      w.write("""[{""")
      schema.approximateRowCount.foreach { count =>
          w.write(s""""approximate_row_count":${JNumber(count)}""")
          w.write("\n ,")
      }
      w.write(s""""locale":${JString(schema.locale)}""")
      w.write('\n')
      schema.pk.foreach { pk =>
        w.write(s""" ,"pk":${JString(pk.name)}""")
        w.write('\n')
      }
      schema.rowCount.foreach { count =>
        w.write(s""" ,"row_count":${JNumber(count)}""")
        w.write('\n')
      }
      w.write(s""" ,"schema":[""")
      var didOne = false
      for(i <- 0 until schemaOrdering.length) {
        if(didOne) w.write(',')
        else didOne = true
        val ci = schema.schema(schemaOrdering(i))
        w.write(s"""{"c":${JString(ci.fieldName.name)},"t":${JsonCodec.toJValue(ci.typ)}}""")
      }
      w.write("]\n }\n")

      // end of header

      val reps: Array[JsonColumnWriteRep] = schema.schema.map { ci => JsonColumnRep.forClientType(ci.typ) }.toArray
      for(row <- rows) {
        w.write(",[")
        if(row.length > 0) {
          jw.write(reps(schemaOrdering(0)).toJValue(row(schemaOrdering(0))))
          var i = 1
          while(i < row.length) {
            w.write(',')
            jw.write(reps(schemaOrdering(i)).toJValue(row(schemaOrdering(i))))
            i += 1
          }
        }
        w.write("]\n")
      }
      w.write("]\n")
    }
  }
}

object CsvExporter extends Exporter {
  val mimeTypeBase = "text/csv"
  val mimeType = new MimeType(mimeTypeBase)
  val extension = Some("csv")

  def export(resp: HttpServletResponse, charset: AliasedCharset, schema: ExportDAO.CSchema, rows: Iterator[Array[SoQLValue]], singleRow: Boolean = false) {
    val mt = new MimeType(mimeTypeBase)
    mt.setParameter("charset", charset.alias)
    resp.setContentType(mt.toString)
    for {
      rawWriter <- managed(resp.getWriter)
      w <- managed(new BufferedWriter(rawWriter, 65536))
    } yield {
      class Processor {
        val writer = w
        val reps: Array[CsvColumnWriteRep] = schema.schema.map { f => CsvColumnRep.forType(f.typ) }.toArray
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
          val array = schema.schema.map(_.humanName).toArray
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
